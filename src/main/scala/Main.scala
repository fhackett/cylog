package ca.uwaterloo.cylog

import scala.collection.mutable.{ArrayBuffer,HashMap,HashSet,ArrayDeque,TreeSet}
import java.nio.file.{Files,Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Using,Try,Success,Failure}

object Main extends App {

  val TIME_UNITS = List("ns", "μs", "ms", "s")
  
  def prettyTime(base : Float, prefixes : List[String]) : String =
    prefixes match {
      case Nil => ??? // no prefixes is invalid
      case hd :: Nil =>
        s"$base $hd"
      case hd :: tl =>
        if( base > 1000 ) {
          prettyTime(base/1000, tl)
        } else {
          s"$base $hd"
        }
    }

  args match {
    case Array(filename) => {
      val input = try {
        val raw = Files.readAllBytes(Paths.get(filename))
        new String(raw, StandardCharsets.UTF_8)
      } catch {
        case e : java.io.IOException => {
          println(s"Error reading file: ${e.getLocalizedMessage()}")
          sys.exit(1)
        }
      }
      val directives = Parser.tryParse(input)
      println("Parsed Datalog directives")

      val blocks = Compiler.compile(directives)
      for(block <- blocks) {
        println("---BLOCK---")
        for((head, dependsOn, part) <- block) {
          println(s"HEAD: $head, dependsOn: $dependsOn")
          println(part)
          println("--")
        }
      }
      import org.neo4j.driver.v1.{AuthTokens,Driver,GraphDatabase,Session,StatementResult,TransactionConfig}

      Using.Manager { use =>
        val driver = use(GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none()))
        val session = use(driver.session())

        val queryStart = System.nanoTime()
        try {
          for(block <- blocks) {
            // this section of code computes a flipped dependency relation between Datalog facts.
            // if one Datalog fact uses some set of others, this algorithm computes, given new instances of some relation F,
            // all relations F' that need recomputing based on it.
            // Consider especially: if a relation has two cases F1 and F2, and only F1 depends on a relation G, only F1 will be computed when
            // G changes.
            val headIds : Map[String,ArrayBuffer[Int]] = locally {
              val headIds = new HashMap[String,(HashSet[Int],ArrayBuffer[Int])]()
              for(((head, dependsOn, cypher), id) <- block.zipWithIndex) {
                val (idSet, ids) = headIds.getOrElseUpdate(head, (new HashSet[Int](), new ArrayBuffer[Int]()))
                if( !idSet(id) ) {
                  ids += id
                  idSet += id
                }
              }
              headIds.map({ case (head, (idSet, ids)) => (head, ids) }).toMap
            }

            val todo = new ArrayDeque[Int]()

            val cypherMap = new HashMap[Int,String]()
            val affects = new HashMap[Int,TreeSet[Int]]()
            val nameOf = new HashMap[Int,String]()
            val indexOfName = new HashMap[String,Int]()
            for(((head, dependsOn, cypher), id) <- block.zipWithIndex) {
              todo += id
              cypherMap(id) = cypher
              // generate names that make sense in context of the Datalog (i.e idx 0 is first definition, 1 is second, etc...)
              val idx = indexOfName.getOrElseUpdate(head, 0)
              indexOfName(head) += 1
              nameOf(id) = s"$head@$idx"

              for(depName <- dependsOn if headIds.contains(depName); dep <- headIds(depName)) {
                val affectsSet = affects.getOrElseUpdate(dep, new TreeSet[Int]())
                affectsSet += id
              }
            }

            //println(s"Affects: ${affects.map({ case (from, to) => s"${nameOf(from)}: ${to.map(nameOf).mkString(", ")}"}).mkString("\n- ")}")

            val isTodo = new HashSet[Int]()
            isTodo ++= todo

            // Logic: if a relation produced changes in the DB and has dependencies, append all its dependencies to the todo queue,
            // but only if they aren't already there (shown by isTodo). Continue until fixpoint is reached, that is, the entire remaining
            // queue produces no DB changes, scheduling no more relations.
            while(!todo.isEmpty) {
              val id = todo.removeHead()
              isTodo -= id
              val cypher = cypherMap(id)

              var shouldSchedule = false
              var wasProductive = false

              println(s"Running fragment ${nameOf(id)}...")

              val fragmentStart = System.nanoTime()
              do {
                val blockStart = System.nanoTime()
                val result = session.run(cypher, TransactionConfig.empty())
                val summary = result.consume()
                val blockEnd = System.nanoTime()
                val counters = summary.counters()
                wasProductive = counters.nodesCreated() + counters.relationshipsCreated() > 0
                shouldSchedule |= wasProductive

                println(s"Block of ${nameOf(id)} generated ${counters.nodesCreated()} nodes and ${counters.relationshipsCreated()} edges, took ${prettyTime(blockEnd-blockStart, TIME_UNITS)}.")
              } while( wasProductive ) 
              val fragmentEnd = System.nanoTime()

              println(s"Fragment ${nameOf(id)} took ${prettyTime(fragmentEnd-fragmentStart, TIME_UNITS)}.")

              if( shouldSchedule && affects.contains(id) ){
                for(affected <- affects(id) if !isTodo(affected)) {
                  println(s"Scheduling affected fragment: ${nameOf(affected)}.")
                  todo += affected
                  isTodo += affected
                }
              }
            }
          }
        } finally {
          val queryEnd = System.nanoTime()
          println(s"Execution time: ${prettyTime(queryEnd-queryStart, TIME_UNITS)}")
        }
      } match {
        case Success(()) => {
          println("Success.")
          sys.exit(0)
        }
        case Failure(e) => {
          println(s"Aborting, error executing query: ${e.getMessage}")
          e.printStackTrace()
          sys.exit(1)
        }
      }
    }
    case _ => {
      println("Gremlog, a Datalog to Apache Gremlin compiler")
      println("USAGE: <gremlog> FILE.dl")
      sys.exit(1)
    }
  }
}

package ca.uwaterloo.cylog

import scala.collection.mutable.{ArrayBuffer,HashMap,HashSet,ArrayDeque,TreeSet}
import java.nio.file.{Files,Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Using,Try,Success,Failure}

object Main extends App {
  
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

        val t0 = System.nanoTime()
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
            for(((head, dependsOn, cypher), id) <- block.zipWithIndex) {
              todo += id
              cypherMap(id) = cypher
              nameOf(id) = head
              for(depName <- dependsOn if headIds.contains(depName); dep <- headIds(depName)) {
                val affectsSet = affects.getOrElseUpdate(dep, new TreeSet[Int]())
                affectsSet += id
              }
            }

            val isTodo = new HashSet[Int]()
            isTodo ++= todo

            // Logic: if a relation produced changes in the DB and has dependencies, append all its dependencies to the todo queue,
            // but only if they aren't already there (shown by isTodo). Continue until fixpoint is reached, that is, the entire remaining
            // queue produces no DB changes, scheduling no more relations.
            while(!todo.isEmpty) {
              val id = todo.removeHead()
              isTodo -= id
              val cypher = cypherMap(id)

              val innert0 = System.nanoTime()
              val result = session.run(cypher, TransactionConfig.empty())
              val summary = result.consume()
              val innert1 = System.nanoTime()

              val counters = summary.counters()
              // This is useful for huge queries, so you can see sort of what Cylog is doing by reading the console.
              // Also helps in telling the difference between the DB being stuck on a huge query vs. Cylog iterating
              println(s"Fragment ${nameOf(id)}@$id completed in ${prettyTime(innert1-innert0, List("ns", "μs", "ms", "s"))}. Created ${counters.nodesCreated()} nodes, ${counters.relationshipsCreated()} edges.")

              if( counters.nodesCreated() + counters.relationshipsCreated() > 0 ){
                for(affected <- affects(id) if !isTodo(affected)) {
                  println(s"Scheduling rerun: ${nameOf(affected)}@$affected")
                  todo += affected
                  isTodo += affected
                }
              }
            }
          }
        } finally {
          val t1 = System.nanoTime()
          println(s"Execution time: ${prettyTime(t1-t0, List("ns", "μs", "ms", "s"))}")
        }
      } match {
        case Success(()) => {
          println("Success.")
          sys.exit(0)
        }
        case Failure(e) => {
          println(s"Aborting, error executing query: ${e.getMessage}")
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

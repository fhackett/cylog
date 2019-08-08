package ca.uwaterloo.gremlog

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
      /*for(block <- blocks) {
        println("---BLOCK---")
        for(part <- block) {
          println(part)
          println("--")
        }
      }*/
      import org.neo4j.driver.v1.{AuthTokens,Driver,GraphDatabase,Session,StatementResult,TransactionConfig}

      Using.Manager { use =>
        val driver = use(GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none()))
        val session = use(driver.session())

        val t0 = System.nanoTime()
        try {
          for(block <- blocks) {
            var continue = true;
            while(continue) {
              continue = false;
              for(part <- block) {
                val result = session.run(part, TransactionConfig.empty())
                val summary = result.consume()
                val counters = summary.counters()
                continue |= counters.nodesCreated() + counters.relationshipsCreated() > 0
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

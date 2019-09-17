package ca.uwaterloo.cylog

import scala.collection.mutable.{ArrayBuffer,HashMap,HashSet,ArrayDeque,TreeSet}
import java.nio.file.{Files,Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Using,Try,Success,Failure}

object Main {

  val TIME_UNITS = List("ns", "μs", "ms", "s")
  
  def prettyTime(base : Float, prefixes : List[String] = TIME_UNITS) : String =
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

  def main(argv : Array[String]) : Unit =
    argv match {
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
          Runner.run(blocks, driver.session())
        } match {
          case Success(()) => {
            println("Success.")
            sys.exit(0)
          }
          case f @ Failure(e) => {
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

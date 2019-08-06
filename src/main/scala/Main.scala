package ca.uwaterloo.gremlog

import java.nio.file.{Files,Paths}
import java.nio.charset.StandardCharsets

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

      val queries = Compiler.compile(directives)
      for(block <- queries) {
        println("---BLOCK---")
        for(part <- block) {
          println(part)
          println("--")
        }
      }

      val t0 = System.nanoTime()
      val t1 = System.nanoTime()
      println(s"Execution time: ${prettyTime(t1-t0, List("ns", "μs", "ms", "s"))}")
      
    }
    case _ => {
      println("Gremlog, a Datalog to Apache Gremlin compiler")
      println("USAGE: <gremlog> FILE.dl")
      sys.exit(1)
    }
  }
}

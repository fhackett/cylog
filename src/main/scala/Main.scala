package ca.uwaterloo.gremlog

import java.nio.file.{Files,Paths}
import java.nio.charset.StandardCharsets

object Main extends App {
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
      println(directives)
      val ir = Solver.convertToIR(directives)
      println(ir)
    }
    case _ => {
      println("Gremlog, a Datalog to Apache Gremlin compiler")
      println("USAGE: <gremlog> FILE.dl")
      sys.exit(1)
    }
  }
  /*val builder = OParser.builder[Config]
  val argParser = {
    import builder._
    OParser.sequence(
      programName("gremlog"),
      head("gremlog", "0.1"),
      arg[JFile]("<idb>").action((x, c) => c.copy(file=File(x.getPath()))),
    )
  }
  val results : Option[Unit] = for(
      config <- OParser.parse(argParser, args, Config());
      facts <- Parser.tryParse(config.file);
      answers <- {
        import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
        val graph = TinkerGraph.open()
        val g = graph.traversal()
        Solver.solve(facts, g)
      })
    yield {
      import collection.JavaConverters._
      println("Results:")
      for(answer <- answers.asScala) {
        println(answer)
      }
    }
  results match {
    case None => sys.exit(1)
    case Some(()) => ()
  }*/
}

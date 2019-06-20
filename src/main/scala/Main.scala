package ca.uwaterloo.gremlog

import better.files._
//import scopt.OParser
import java.io.{File => JFile}
import com.typesafe.scalalogging.Logger

case class Config(
  file : File = File("."),
)

object Main extends App {
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

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
      val blocks = Solver.convertToIR(directives)
      println(blocks)
      import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
      val graph = TinkerGraph.open()
      val g = graph.traversal()
      val traversal = Solver.generateTraversal(blocks, g)
      println(traversal.explain())
      println(traversal.dedup().valueMap(true).toList())
    }
    case _ => {
      println("Gremlog, a Datalog to Apache Gremlin compiler")
      println("USAGE: <gremlog> FILE.dl")
      sys.exit(1)
    }
  }
}

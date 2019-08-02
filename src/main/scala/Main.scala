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
      //println(directives)
      val blocks = Solver.convertToIR(directives)
      println("Converted directives to IR")
      //println(blocks)
      // import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
      // val graph = TinkerGraph.open()
      import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
      val graph = Neo4jGraph.open("./testgraph")

      val g = graph.traversal()

      graph.tx().open()
      g.V().drop().iterate()
      graph.tx().commit()

      graph.cypher("CREATE INDEX ON :Y(i)")

      graph.tx().open()
      for(i <- 0 until 100) {
        g.addV("X").property("i", i).iterate()
      }
      graph.tx().commit()

      graph.tx().open()
      val traversal = Solver.generateTraversal(blocks, g)
      println("Generated Gremlin traversal, executing")
      //println(traversal.explain())

      val t0 = System.nanoTime()
      traversal.iterate()
      val t1 = System.nanoTime()
      println(s"Execution time: ${prettyTime(t1-t0, List("ns", "μs", "ms", "s"))}")
      graph.tx().commit()
      graph.close()
      /*val vmap = traversal.valueMap(true)
      while(vmap.hasNext()) {
        println(vmap.next())
      }*/
      
      // g.io("output.xml").write().iterate()
    }
    case _ => {
      println("Gremlog, a Datalog to Apache Gremlin compiler")
      println("USAGE: <gremlog> FILE.dl")
      sys.exit(1)
    }
  }
}

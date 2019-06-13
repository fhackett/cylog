package ca.uwaterloo.gremlog

import better.files._
import scopt.OParser
import java.io.{File => JFile}
import com.typesafe.scalalogging.Logger

case class Config(
  file : File = File("."),
)

object Main extends App {
  val builder = OParser.builder[Config]
  val argParser = {
    import builder._
    OParser.sequence(
      programName("gremlog"),
      head("gremlog", "0.1"),
      arg[JFile]("<idb>").action((x, c) => c.copy(file=File(x.getPath()))),
    )
  }
  val results = for(
    config <- OParser.parse(argParser, args, Config());
    facts <- Parser.tryParse(config.file);
    answers <- {
      import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
      import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversalSource,__,GraphTraversal};
      import org.apache.tinkerpop.gremlin.process.traversal._;
      import org.apache.tinkerpop.gremlin.structure.Vertex;
      val graph = TinkerGraph.open()
      val g = graph.traversal()

      // use select + where to ensure multiple occurrences of a var are equal
      // use has and values to extract data
      // i cri evry tiem when match does not unify non-vertices

      //g.addV("dummy1").property("a", 1).iterate
      //g.addV("dummy2").property("b", 2).iterate
      //println(g.V().has("a").as("x").values("a").as("aa").V().has("b").as("y").values("b").as("bb").select("aa", "bb").where("aa", P.eq("bb")).select("x", "y").toList)
      //g.addV("dummy2").iterate
      /*g.addV("$intLiteral").property("$int",2).iterate*/
      //g.addV("$DUMMY").property("foo", 42).iterate
      /*println(g.V().`match`(
        __.as("x").label().as("label"),
        __.as("x").properties().as("props"))
          .select("label","props").toList)*/
      /*println(g.V().`match`(
        __.as("x").has("foo")
      ).fold().not(__.unfold()).toList)*/
      /*println(g.V().has("$int").fold().not(__.unfold.has("$int",2)).addV("$intLiteral2").property("$int",2).toList)
      g.V().has("$int").fold().union(
        __.not(__.unfold.has("$int",2)).addV("$intLiteral2").property("$int",2)
        ).iterate*/
      /*println(g.V().as("root").`match`(
        __.as("a").has("$int", 2))
          .as("mResult")
          .V().has("foo",42).fold
            //.not(__.unfold())
            .select("mResult").select("a").toList)*/
      /*println(
        g.V().has("$int", 2).as("x").not(__.V().has("foo")).toList)
      println(g.V().`match`(
        __.as("x").label().as("label"),
        __.as("x").properties().as("props"))
          .select("x", "label","props").toList)*/
      Solver.solve(facts, g)
      //Some(List.empty)
    }
  ) yield { println(answers.toList); () }
  results match {
    case None => sys.exit(1)
    case _ => ()
  }
}

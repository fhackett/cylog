package ca.uwaterloo.gremlog

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import collection.mutable.{HashSet,HashMap,ArrayBuffer}

class Key

sealed abstract class Node {
  val key = new Key
}

case class Fact(val name : String, val head : Seq[Value], val conditions : Seq[Relation]) extends Node
case class Relation(val name : String, val params : Seq[Value]) extends Node

sealed abstract class Value extends Node
case class StringValue(val value : String) extends Value
case class IntValue(val value : Int) extends Value
case class NameValue(val name : String) extends Value

object Parser extends StrictLogging {
  import fastparse._, NoWhitespace._

  def name [_ : P] : P[String] = P( (CharIn("a-zA-Z").! ~ (CharsWhileIn("a-zA-Z0-9").!.?).map({ case None => ""; case Some(s) => s}))
                                    .map(vs => vs._1 ++ vs._2) )

  def int [_ : P] : P[Int] = P( (CharIn("0-9").! ~ (CharsWhileIn("0-9").!.?).map({ case None => ""; case Some(s) => s}))
                                .map(vs => Integer.parseInt(vs._1 ++ vs._2)) )
  def intVal [_ : P] : P[IntValue] = P( int.map(i => IntValue(i)) )
  def nameVal [_ : P] : P[NameValue] = P( name.map(NameValue(_)) )
  def string [_ : P] : P[String] = P( ("\"" ~ ("\\\"" | (!"\"" ~ AnyChar) ).rep.! ~ "\"").map(seq => new String(seq)) )
  def stringVal [_ : P] : P[StringValue] = P( string.map(str => StringValue(str)) )

  def value [_ : P] : P[Value] = P( intVal | nameVal | stringVal )

  def ws [_ : P] = P( CharsWhileIn("\r\n\t ").? )

  def relation [_ : P] : P[Relation] =
    P( (name.! ~ ws ~ "(" ~ (ws ~ value).rep(sep=(ws ~ ",")) ~ ")").map(tpl => Relation(tpl._1, tpl._2)) )

  def fact [_ : P] : P[Fact] =
    P( (name.! ~/ ws ~ "(" ~/ (ws ~/ value).rep(sep=(ws ~ ",")) ~ ws ~ ")" ~ ws ~ (":-" ~ (ws ~ relation).rep(sep=(ws ~ ","))).? ~ ws ~ ".")
       .map(tpl => Fact(tpl._1, tpl._2, tpl._3 match { case None => Seq.empty; case Some(s) => s})) )

  def program[_ : P] : P[Seq[Fact]] = P( ws ~ fact.rep(sep=ws) ~ ws ~ End )

  def tryParse(text : String) : Option[Seq[Fact]] =
    parse(text, program(_)) match {
      case Parsed.Success(result, _) => Some(result)
      case f : Parsed.Failure => {
        logger.error(f.trace().longAggregateMsg)
        None
      }
    }

  def tryParse(file : File) : Option[Seq[Fact]] =
    try {
      tryParse(file.contentAsString) match {
        case s @ Some(_) => s
        case None => {
          logger.error(s"Failed to parse file $file")
          None
        }
      }
    } catch {
      case e : java.nio.file.NoSuchFileException => {
        logger.error(s"File not found $file")
        None
      }
    }
}

object Solver extends StrictLogging {
  def checkAllHeadsMentioned(program : Seq[Fact]) : Option[Seq[Fact]] = {
    val good = program.forall(fact => {
      val heads = new HashSet[String]()
      val headCheck =
        fact.head.forall({
          case NameValue(name) => {
            if( !heads(name) ) {
              heads += name
              true
            } else {
              logger.error(s"Head variable $name defined more than once in fact ${fact.name}")
              false
            }
          }
          case StringValue(_) => true
          case IntValue(_) => true
        })
      val referencedNames =
        (for(rel <- fact.conditions; NameValue(name) <- rel.params)
          yield name).toSet
      
      val unreferencedNames = heads &~ referencedNames
      val bodyCheck =
        if( !unreferencedNames.isEmpty ) {
          logger.error(s"Head variables $unreferencedNames not mentioned in body of fact ${fact.name}")
          false
        } else {
          true
        }

      headCheck && bodyCheck
    })
    if( good ) Some(program) else None
  }

  /*def findLiterals(program : Seq[Fact]) : Seq[Value] =
    (for(fact <- program;
        v <- fact.head ++ fact.conditions.flatMap(_.params) if( v match {
          case IntValue(_)|StringValue(_) => true
          case NameValue(_) => false
        }))
      yield v).distinct*/

  /*def accumulateBindings(program : Seq[Fact]) : Seq[Map[String,Int]] =
    program.map({
      case Fact(name, head, conditions) =>
        conditions.foldLeft(Map.empty)((m, rel) => {
          rel.params.foldLeft(m)((m, v) => v match {
            case IntValue(_) => m
            case StringValue(_) => m
            case NameValue(n) => m.updated(n, m.getOrElse(n, 0)+1)
          })
        })
    })*/
  
  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversalSource,__,GraphTraversal};
  import org.apache.tinkerpop.gremlin.process.traversal._;
  import org.apache.tinkerpop.gremlin.structure.Edge;
  import org.apache.tinkerpop.gremlin.structure.Vertex;
  import org.apache.tinkerpop.gremlin.structure.io.IoCore;

  def generateTraversal(program : Seq[Fact], g : GraphTraversalSource) : Option[GraphTraversal[_,_]] = {

    def genMatchCase(c : Int, name : String, conds : Seq[Value]) : Seq[GraphTraversal[_ <: Vertex,_]] = {
      val first : Seq[GraphTraversal[_ <: Vertex,_]] = conds.zipWithIndex.map((tpl : (Value,Int)) => tpl match {
        case (IntValue(v), i) => __.as(s"$$anon$c").has(i.toString, v)
        case (StringValue(v), i) => __.as(s"$$anon$c").has(i.toString, v)
        case (NameValue(v), i) => __.as(s"$$anon$c").has(i.toString).values(i.toString).as(v)
      })
      first ++ Seq(__.as(s"$$anon$c").hasLabel(name))
    }

    def genProps[A](src : GraphTraversal[A,Vertex], head : Seq[Value]) : GraphTraversal[A,Vertex] =
      head.zipWithIndex.foldLeft(src)((t, hd) => hd match {
        case (IntValue(v), i) => t.property(i.toString, v)
        case (StringValue(v), i) => t.property(i.toString, v)
        case (NameValue(v), i) => t.property(i.toString, __.select(s"${v}0"))
      })

    def genInstCase[A,B](src : GraphTraversal[A,B], name : String, head : Seq[Value]) : GraphTraversal[A,Vertex] =
      genProps(src.addV(name), head)

    //val literals = findLiterals(program)
    //val (bases, derived) = program.partition(fact => fact.conditions.isEmpty)

    /*val setupLiterals : GraphTraversal[Vertex,Vertex] =
      g.V().sideEffect(t => println(s"t $t")).or(
        literals.map({
          case IntValue(v) =>
            __.coalesce(
              __.has("$int", v),
              __.addV("$intLiteral").property("$int", v)).as(s"$$intLiteral$v")
          case StringValue(v) =>
            __.coalesce(
              __.has("str", v),
              __.addV("$strLiteral").property("$str", v)).as(s"$$strLiteral$v")
          case NameValue(_) => ???
        }) :_*)
    Some(setupLiterals)*/
    /*val setupBases : GraphTraversal[Vertex,Vertex] =


    val setup : GraphTraversal[Vertex,Vertex] =
      if( bases.isEmpty ) {
        g.V()
      } else {
        val s = genProps(g.addV(bases.head.name), bases.head.head)
        bases.tail.foldLeft(s)((t, fact) => genInstCase(t, fact.name, fact.head)).V()
      }
    Some(setup.sideEffect(t => println(t)).repeat(
      __.or(
        derived.map(fact => {
          val search : GraphTraversal[Vertex,java.util.Map[String,Any]] =
            __.`match`[Vertex,Any](
              fact.conditions.zipWithIndex.flatMap({
                case (cond, i) => genMatchCase(i, cond.name, cond.params)
              }) ++ Seq(
                __.not(__.or(genMatchCase(fact.conditions.length, fact.name, fact.head):_*))
              ):_*)
          genInstCase(search, fact.name, fact.head)
        }) :_*
      )
    ).times(2).emit)*/
    /*val setupBases =
      g.V().union(
        bases.zipWithIndex.map({
          case Fact(name, head, conditions) =>

        }) ++ Seq(__.identity()) :_*)
      if( !bases.isEmpty ) {
        val hd = bases.head.head.zipWithIndex.foldLeft[GraphTraversal[Vertex,Vertex]](g.addV(fact.name))((t, v) => v match {
          case (IntValue(v), i)
        })
      } else {
        g.V()
      }*/
    Some(g.V().hasLabel("$$$DUMMY$$$").fold().coalesce(__.unfold(), __.addV("$$$DUMMY$$$")).repeat(
      __.union(
        program.map(fact => {
          if( fact.conditions.isEmpty ) {
            val findHead = fact.head.zipWithIndex.foldLeft[GraphTraversal[Any,Vertex]](__.V().hasLabel(fact.name))((t, tp) => tp match {
              case (IntValue(v), i) => t.has(i.toString, v)
              case (StringValue(v), i) => t.has(i.toString, v)
              case (NameValue(_), _) => ???
            })
            genInstCase(findHead.fold().not(__.unfold()), fact.name, fact.head)
          } else {
            val boundVars = new HashMap[String,Int]
            def performSearch(t : GraphTraversal[Vertex,Vertex], cond : Relation) : GraphTraversal[Vertex,Vertex] = cond match {
              case Relation(name, params) => {
                params.zipWithIndex.foldLeft[GraphTraversal[Vertex,Vertex]](t.hasLabel(name))((t, v) => v match {
                  case (IntValue(v), i) => t.has(i.toString, v)
                  case (StringValue(v), i) => t.has(i.toString, v)
                  case (NameValue(v), i) => {
                    val idx = boundVars.getOrElse(v, 0)
                    boundVars.update(v, idx + 1)
                    t.has(i.toString).as("$interim").values(i.toString).as(s"$v$idx").select("$interim")
                  }
                })
              }
            }
            val search : GraphTraversal[Vertex,Vertex] =
              fact.conditions.tail.foldLeft[GraphTraversal[Vertex,Vertex]](performSearch(__.V(), fact.conditions.head))((t, cond) =>
                  performSearch(t.V(),cond))

            val eqToCheck =
              (for(
                   (name, count) <- boundVars.toSeq;
                   i <- 1 until count)
                 yield (name, i)).toSeq
            val eqChecks =
              if( !eqToCheck.isEmpty ) {
                search.filter(eqToCheck.foldLeft[GraphTraversal[Vertex,java.util.Map[String,Object]]]({
                  val names = (for(
                      (name, count) <- boundVars.toSeq;
                      i <- 0 until count)
                    yield s"$name$i").toSeq
                  __.identity().select[Object](names.head, names.tail.head, names.tail.tail :_*)
                })((t, tp) => tp match {
                  case (name, i) => t.where(s"${name}0", P.eq(s"$name$i"))
                }))
              } else {
                search
              }
            val findHead = {
              val toCheck = new ArrayBuffer[String]
              val find = fact.head.zipWithIndex.foldLeft(__.V().hasLabel(fact.name))((t, tp) => tp match {
                case (IntValue(v), i) => t.has(i.toString, v)
                case (StringValue(v), i) => t.has(i.toString, v)
                case (NameValue(v), i) => {
                  toCheck += v
                  t.has(i.toString).as("$interim").values(i.toString).as(s"$v${boundVars(v)}").select("$interim")
                }
              })
              if( !toCheck.isEmpty ) {
                val toSelect = toCheck.flatMap(n => Seq(s"${n}0", s"$n${boundVars(n)}"))
                val selected = find.select(toSelect.head, toSelect.tail.head, toSelect.tail.tail :_*)
                toCheck.foldLeft(selected)((t, n) => t.where(s"${n}0", P.eq(s"$n${boundVars(n)}")))
              } else {
                find
              }
            }
            genInstCase(
              /*{
                val toSelect = boundVars.keys.map(n => s"${n}0").toSeq
                val filtered = */eqChecks.filter(findHead.fold().not(__.unfold()))
                /*if( toSelect.length < 2 ) {
                  filtered
                } else {
                  filtered.select(toSelect.head, toSelect.tail.head, toSelect.tail.tail :_*)
                }
              }*/,
              fact.name,
              fact.head)
          }
        }) :_*
      )
    ).emit)
  }

  def solve(program : Seq[Fact], graph : GraphTraversalSource) : Option[Iterator[Any]] =
    for(checked <- checkAllHeadsMentioned(program);
        traversal <- generateTraversal(checked, graph))
      yield {
        import scala.collection.JavaConverters
        println(traversal.explain)
        println(traversal.toList())
        /*println(graph.V().`match`(
          __.as("x").label().as("label"),
          __.as("x").properties().as("properties")
        ).select("x", "label", "properties").toList())*/
        println(graph.V().valueMap(true).toList)
        JavaConverters.asScalaIterator(traversal)
      }

}


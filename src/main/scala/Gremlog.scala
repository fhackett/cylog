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
  
  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversalSource,__,GraphTraversal};
  import org.apache.tinkerpop.gremlin.process.traversal._;
  import org.apache.tinkerpop.gremlin.structure.Edge;
  import org.apache.tinkerpop.gremlin.structure.Vertex;
  import org.apache.tinkerpop.gremlin.structure.io.IoCore;

  def generateTraversal(program : Seq[Fact], g : GraphTraversalSource) : Option[GraphTraversal[Vertex,Vertex]] = {

    def genProps[A](src : GraphTraversal[A,Vertex], head : Seq[Value]) : GraphTraversal[A,Vertex] =
      head.zipWithIndex.foldLeft(src)((t, hd) => hd match {
        case (IntValue(v), i) => t.property(i.toString, v)
        case (StringValue(v), i) => t.property(i.toString, v)
        case (NameValue(v), i) => t.property(i.toString, __.select(s"${v}0"))
      })

    def genInstCase[A,B](src : GraphTraversal[A,B], name : String, head : Seq[Value]) : GraphTraversal[A,Vertex] =
      genProps(src.addV(name), head)

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
              eqChecks.filter(findHead.fold().not(__.unfold())),
              fact.name,
              fact.head)
          }
        }) :_*
      )
    ).emit)
  }

  def solve(program : Seq[Fact], graph : GraphTraversalSource) : Option[GraphTraversal[Vertex,java.util.Map[Object,Object]]] =
    for(checked <- checkAllHeadsMentioned(program);
        traversal <- generateTraversal(checked, graph))
      yield traversal.dedup().valueMap(true)
}


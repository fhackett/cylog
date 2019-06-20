package ca.uwaterloo.gremlog

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import collection.mutable.{HashSet,HashMap,ArrayBuffer,ArrayDeque}

class ID

sealed abstract class Type
case object NumberType extends Type
case object SymbolType extends Type
case object NodeType extends Type

sealed abstract class Identifiable {
  val id = new ID
}

sealed abstract class Binding extends Identifiable
case class StringBinding(val value : String) extends Binding
case class IntBinding(val value : Int) extends Binding
case class NameBinding(val name : String) extends Binding
case class WildcardBinding() extends Binding

sealed abstract class Condition extends Identifiable
case class Relation(val name : String, val bindings : Seq[Binding]) extends Condition

sealed abstract class Directive extends Identifiable
case class Fact(val name : String, val head : Seq[Binding], val conditions : Seq[Condition]) extends Directive
sealed abstract class Declaration extends Directive
case class PlainDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration
case class NodeDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration
case class EdgeDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration

object Parser extends StrictLogging {
  import fastparse._, NoWhitespace._

  def name [_ : P] : P[String] = P( (CharIn("a-zA-Z").! ~ (CharsWhileIn("a-zA-Z0-9").!.?).map({ case None => ""; case Some(s) => s}))
                                    .map(vs => vs._1 ++ vs._2) )
  def wildcard [_ : P] = P( CharsWhileIn("_") ~/ name.? )

  def int [_ : P] : P[Int] = P( (CharIn("0-9").! ~ (CharsWhileIn("0-9").!.?).map({ case None => ""; case Some(s) => s}))
                                .map(vs => Integer.parseInt(vs._1 ++ vs._2)) )
  def intBinding [_ : P] : P[IntBinding] = P( int.map(i => IntBinding(i)) )
  def nameBinding [_ : P] : P[NameBinding] = P( name.map(NameBinding(_)) )
  def wildcardBinding [_ : P] : P[WildcardBinding] = P( wildcard.map(_ => WildcardBinding()) )
  def string [_ : P] : P[String] = P( ("\"" ~ ("\\\"" | (!"\"" ~ AnyChar) ).rep.! ~ "\"").map(seq => new String(seq)) )
  def stringBinding [_ : P] : P[StringBinding] = P( string.map(str => StringBinding(str)) )

  def bodyBind [_ : P] : P[Binding] = P( intBinding | nameBinding | stringBinding | wildcardBinding )
  def headBind [_ : P] : P[Binding] = P( intBinding | nameBinding | stringBinding )

  def ws [_ : P] = P( CharsWhileIn("\r\n\t ").? )
  def Ws [_ : P] = P( CharsWhileIn("\r\n\t ") )

  def tpe [_ : P] : P[Type] =
    P( "number".!.map(_ => NumberType) | "symbol".!.map(_ => SymbolType) | "node".!.map(_ => NodeType) )

  def nameType [_ : P] : P[(String,Type)] = P( name.! ~ ws ~ ":" ~ ws ~ tpe )
  def declaration [_ : P] : P[PlainDeclaration] =
    P(
      (".decl" ~ Ws ~ name.! ~ ws ~ "(" ~ ws ~ nameType.rep(sep=(ws ~ ",")) ~ ")").map({
        case (name, types) => PlainDeclaration(name, types)
      }))

  def nodeDecl [_ : P] : P[NodeDeclaration] =
    P(
      (".node" ~ Ws ~ name.! ~ ws ~ "(" ~ ws ~ wildcard ~ ws ~ "," ~ ws ~ nameType.rep(sep=(ws ~ ",")) ~ ")").map({
        case (name, _, types) => NodeDeclaration(name, types)
      }))
  def edgeDecl [_ : P] : P[EdgeDeclaration] =
    P(
      (".edge" ~ Ws ~ name.! ~ ws ~ "(" ~ ws ~ wildcard ~ ws ~ "," ~ ws ~ wildcard ~ ws ~ "," ~ ws ~ nameType.rep(sep=(ws ~ ",")) ~ ")").map({
        case (name, _, _, types) => EdgeDeclaration(name, types)
      }))

  def relation [_ : P] : P[Relation] =
    P( (name.! ~ ws ~ "(" ~ (ws ~ bodyBind).rep(sep=(ws ~ ",")) ~ ")").map(tpl => Relation(tpl._1, tpl._2)) )

  def condition [_ : P] : P[Condition] = P( relation )

  def fact [_ : P] : P[Fact] =
    P( (name.! ~/ ws ~ "(" ~/ (ws ~/ headBind).rep(sep=(ws ~ ",")) ~ ws ~ ")" ~ ws ~ (":-" ~ (ws ~ condition).rep(sep=(ws ~ ","))).? ~ ws ~ ".")
       .map(tpl => Fact(tpl._1, tpl._2, tpl._3 match { case None => Seq.empty; case Some(s) => s})) )

  def directive [_ : P] : P[Directive] = P( fact | declaration | nodeDecl | edgeDecl )

  def program[_ : P] : P[Seq[Directive]] = P( ws ~ fact.rep(sep=Ws) ~ ws ~ End )

  def tryParse(text : String) : Option[Seq[Directive]] =
    parse(text, program(_)) match {
      case Parsed.Success(result, _) => Some(result)
      case f : Parsed.Failure => {
        logger.error(f.trace().longAggregateMsg)
        None
      }
    }

  def tryParse(file : File) : Option[Seq[Directive]] =
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

sealed abstract class UnifyTypeV
case class UnifyTypeVType(val tpe : Type) extends UnifyTypeV
case class UnifyTypeVID(val id : ID) extends UnifyTypeV

sealed abstract class TypeConstraint
case class UnifyIDIDType(val id1 : ID, val id2 : ID) extends TypeConstraint
case class UnifyIDType(val id : ID, val tpe : Type) extends TypeConstraint
case class UnifyTypeType(val tpe1 : Type, val tpe2 : Type) extends TypeConstraint
case class UnifyTypeSeq(val s1 : Seq[UnifyTypeV], val s2 : Seq[UnifyTypeV]) extends TypeConstraint

object Typer {
  def unify(constraints : Seq[TypeConstraint]) : Map[ID, Type] = {
    val todo = new ArrayDeque[TypeConstraint]
    todo ++= constraints
    val types = new HashMap[ID,Type]
    while(!todo.isEmpty) {
      todo.removeHead() match {
        case UnifyTypeSeq(s1, s2) => {
          if( s1.length != s2.length ) {
            ??? // arity mismatch
          }
          todo ++= (s1 zip s2).map({
            case (UnifyTypeVType(tpe1), UnifyTypeVType(tpe2)) => UnifyTypeType(tpe1, tpe2)
            case (UnifyTypeVID(id), UnifyTypeVType(tpe)) => UnifyIDType(id, tpe)
            case (UnifyTypeVType(tpe), UnifyTypeVID(id)) => UnifyIDType(id, tpe)
            case (UnifyTypeVID(id1), UnifyTypeVID(id2)) => UnifyIDIDType(id1, id2)
          })
        }
        case UnifyIDType(id, tpe) => {
          if( types.contains(id) ) {
            todo += UnifyTypeType(types(id), tpe)
          } else {
            types(id) = tpe
          }
        }
        case UnifyIDIDType(id1, id2) => {
          if( types.contains(id1) ) {
            todo += UnifyIDType(id2, types(id1))
          } else if( types.contains(id2) ) {
            todo += UnifyIDType(id1, types(id2))
          } else {
            // only terminates if typing is solvable
            todo += UnifyIDIDType(id1, id2)
          }
        }
        case UnifyTypeType(tpe1, tpe2) => {
          if( tpe1 != tpe2 ) {
            ??? // type mismatch
          }
        }
      }
    }
    types.toMap
  }
}

object Solver extends StrictLogging {

  def resolveScopes(directives : Seq[Directive]) : (Map[ID,String],Map[ID,ID],Map[ID,ID],Map[ID,Seq[ID]]) = {
    val decls = new ArrayBuffer[Declaration]
    val facts = new ArrayBuffer[Fact]

    directives.foreach({
      case d : Declaration => decls += d
      case f : Fact => facts += f
    })

    val labelOf = new HashMap[ID,String]
    val getDeclaration = new HashMap[ID,Declaration]
    val refersTo = new HashMap[ID,ID]
    val definedBy = new HashMap[ID,ID]
    val declMembers = new HashMap[ID,Seq[ID]]

    val factIDs = new HashMap[String, ID]

    val alreadyDeclared = new HashSet[String]
    def makeDecl(id : ID, name : String, types : Seq[(String,Type)]) : Unit = {
      if( alreadyDeclared(name) ) {
        ??? // cannot declare relation twice
      } else {
        alreadyDeclared += name
        labelOf(id) = name
        val mentioned = new HashSet[String]
        val ids = new ArrayBuffer[ID]
        types.foreach({
          case (fieldName, tpe) => {
            if( mentioned(fieldName) ) {
              ??? // relation argument names need to be unique
            } else {
              mentioned += fieldName
              val fieldID = new ID
              ids += fieldID
              labelOf(fieldID) = fieldName
            }
          }
        })
        factIDs(name) = id
        declMembers(id) = ids.toSeq
      }
    }
    decls.foreach(d => {
      getDeclaration(d.id) = d
      d match {
        case NodeDeclaration(name, types) =>
          makeDecl(d.id, name, Seq(("$n", NodeType)) ++ types)
        case EdgeDeclaration(name, types) =>
          makeDecl(d.id, name, Seq(("$n1", NodeType), ("$n2", NodeType)) ++ types)
        case PlainDeclaration(name, types) =>
          makeDecl(d.id, name, types)
      }
    })

    facts.foreach(fact => fact match {
      case Fact(name, head, conditions) => {
        if( factIDs.contains(name) ) {
          val headID = factIDs(name)
          val headIDs = declMembers(headID)
          val boundNames = new HashMap[String, ID]
          if( headIDs.length != head.length ) {
            ??? // arity must match between declaration and facts
          }
          definedBy(fact.id) = headID
          (headIDs zip head).foreach({
            case (id, binding) => {
              definedBy(binding.id) = id
              binding match {
                case NameBinding(name) => {
                  if( boundNames.contains(name) ) {
                    ??? // can't bind the same name twice in HEAD
                  }
                  boundNames(name) = binding.id
                  refersTo(binding.id) = binding.id
                }
                case IntBinding(_) => refersTo(binding.id) = binding.id
                case StringBinding(_) => refersTo(binding.id) = binding.id
                case WildcardBinding() => refersTo(binding.id) = binding.id
              }
            }
          })
          conditions.foreach({
            case rel @ Relation(name, bindings) => {
              if( factIDs.contains(name) ) {
                val sigID = factIDs(name)
                val sigHead = declMembers(sigID)
                if( sigHead.length != bindings.length ) {
                  ??? // arity must match between declaration and usage
                }

                definedBy(rel.id) = sigID
                (sigHead zip bindings).foreach({
                  case (id, binding) => {
                    definedBy(binding.id) = id
                    binding match {
                      case NameBinding(name) => {
                        if( boundNames.contains(name) ) {
                          refersTo(binding.id) = boundNames(name)
                        } else {
                          refersTo(binding.id) = binding.id
                          boundNames(name) = binding.id
                        }
                      }
                      case IntBinding(_) => refersTo(binding.id) = binding.id
                      case StringBinding(_) => refersTo(binding.id) = binding.id
                      case WildcardBinding() => refersTo(binding.id) = binding.id
                    }
                  }
                })
              } else {
                ??? // need to declare relations that you reference
              }
            }
          })
        } else {
          ??? // need to declare relations for which you give facts
        }
      }
    })
    
    (labelOf.toMap, refersTo.toMap, definedBy.toMap, declMembers.toMap)
  }

  def resolveTypes(directives : Seq[Directive], refersTo : Map[ID,ID], definedBy : Map[ID,ID], declMembers : Map[ID,Seq[ID]]) : Map[ID,Type] = {
    val constraints = new ArrayBuffer[TypeConstraint]
    
    def constrainBinding(binding : Binding) : Unit = binding match {
      case IntBinding(_) => constraints += UnifyIDType(binding.id, NumberType)
      case StringBinding(_) => constraints += UnifyIDType(binding.id, SymbolType)
      case NameBinding(_) => ()
      case WildcardBinding() => ()
    }
    
    directives.foreach(d => d match {
      case Fact(name, head, conditions) => {
        head.foreach(constrainBinding(_))
        constraints += UnifyTypeSeq(head.map(bind => UnifyTypeVID(bind.id)), head.map(bind => UnifyTypeVID(definedBy(bind.id))))
        conditions.foreach(cond => cond match {
          case Relation(_, bindings) => {
            bindings.foreach(constrainBinding(_))
            constraints += UnifyTypeSeq(bindings.map(bind => UnifyTypeVID(bind.id)), bindings.map(bind => UnifyTypeVID(refersTo(bind.id))))
          }
        })
      }
      case PlainDeclaration(name, types) => {
        constraints += UnifyTypeSeq(types.map(tp => UnifyTypeVType(tp._2)), declMembers(d.id).map(UnifyTypeVID(_)))
      }
      case NodeDeclaration(name, types) => {
        constraints += UnifyTypeSeq(
          Seq(UnifyTypeVType(NodeType)) ++ types.map(tp => UnifyTypeVType(tp._2)),
          declMembers(d.id).map(UnifyTypeVID(_)))
      }
      case EdgeDeclaration(name, types) => {
        constraints += UnifyTypeSeq(
          Seq(UnifyTypeVType(NodeType), UnifyTypeVType(NodeType)) ++ types.map(tp => UnifyTypeVType(tp._2)),
          declMembers(d.id).map(UnifyTypeVID(_)))
      }
    })
    Typer.unify(constraints.toSeq)
  }

  def checkAllHeadsMentioned(facts : Seq[Fact]) : Unit =
    facts.foreach({
      case Fact(_, head, conditions) => {
        val headNames = head.flatMap({
          case IntBinding(_) => Seq.empty
          case StringBinding(_) => Seq.empty
          case NameBinding(name) => Seq(name)
          case WildcardBinding() => Seq.empty
        }).toSet
        val condNames = conditions.flatMap({
          case Relation(name, bindings) => bindings.flatMap({
            case IntBinding(_) => Seq.empty
            case StringBinding(_) => Seq.empty
            case NameBinding(name) => Seq(name)
            case WildcardBinding() => Seq.empty
          })
        }).toSet
        val unreferenced = headNames &~ condNames
        if( !unreferenced.isEmpty ) {
          ??? // all head variables must be referenced in the fact body
        }
      }
    })
  
  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversalSource,__,GraphTraversal};
  import org.apache.tinkerpop.gremlin.process.traversal._;
  import org.apache.tinkerpop.gremlin.structure.Edge;
  import org.apache.tinkerpop.gremlin.structure.Vertex;
  import org.apache.tinkerpop.gremlin.structure.io.IoCore;

  def generateTraversal(facts : Set[Fact], typeOf : Map[ID,Type], labelOf : Map[ID,String], definedBy : Map[ID,ID], g : GraphTraversalSource) : Option[GraphTraversal[Vertex,Vertex]] = {

    ???
    /*var nextName = 0
    val names = new HashMap[ID,String]

    def nameOf(id : ID) : String = {
      if( names.contains(id) ) {
        names(id)
      } else {
        names(id) = nextName.toString
        nextName += 1
        names(id)
      }
    }

    val boundTo = new HashMap[ID,ID]

    def genProps[A](src : GraphTraversal[A,Vertex], head : Seq[Binding]) : GraphTraversal[A,Vertex] =
      head.foldLeft(src)((t, b) => b match {
        case IntValue(v) => t.property(labelOf(definedBy(b.id)), v)
        case StringValue(v) => t.property(labelOf(definedBy(b.id)), v)
        case NameValue(v) => typeOf(b.id) match {
          case IntType|SymbolType => t.property(labelOf(definedBy(b.id)), __.select(nameOf(boundTo(b.id))))
          case NodeType => t.addE(labelOf(definedBy(b.id))).to(s"${v}0").inV()
        }
        case WildcardBinding() => ???
      })

    def genInstCase[A,B](src : GraphTraversal[A,B], name : String, head : Seq[Value]) : GraphTraversal[A,Vertex] =
      genProps(src.addV(name), head)

    Some(g.V().hasLabel("$$$DUMMY$$$").fold().coalesce(__.unfold(), __.addV("$$$DUMMY$$$")).repeat(
      __.union(
        facts.map(fact => {
          if( fact.conditions.isEmpty ) {
            val findHead = fact.head.foldLeft[GraphTraversal[Any,Vertex]](__.V().hasLabel(fact.name))((t, b) => b match {
              case IntBinding(v) => t.has(labelOf(definedBy(b.id)), v)
              case StringBinding(v) => t.has(labelOf(definedBy(b.id)), v)
              case WildcardBinding() => ???
              case NameValue(_) => ???
            })
            genInstCase(findHead.fold().not(__.unfold()), fact.name, fact.head)
          } else {
            val boundVars = new HashMap[String,ArrayBuffer]
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
    ).emit)*/
  }

  def solve(program : Seq[Fact], graph : GraphTraversalSource) : GraphTraversal[Vertex,java.util.Map[Object,Object]] =
    ???
    /*for(checked <- checkAllHeadsMentioned(program);
        traversal <- generateTraversal(checked, graph))
      yield traversal.dedup().valueMap(true)*/
}


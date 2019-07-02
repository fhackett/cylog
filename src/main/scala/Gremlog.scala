package ca.uwaterloo.gremlog

import collection.mutable.{HashSet,HashMap,ArrayBuffer,ArrayDeque}

object AST {
  sealed abstract class Type
  case object NumberType extends Type
  case object SymbolType extends Type
  case object InType extends Type
  case object OutType extends Type
  case object SelfType extends Type

  sealed abstract class Binding
  case class SymbolBinding(val value : String) extends Binding
  case class NumberBinding(val value : Int) extends Binding
  case class NameBinding(val name : String) extends Binding
  case object WildcardBinding extends Binding
  case object IDBinding extends Binding

  sealed abstract class Condition
  case class Relation(val name : String, val bindings : Seq[Binding]) extends Condition

  sealed abstract class Directive
  case class Fact(val name : String, val head : Seq[Binding], val conditions : Seq[Condition]) extends Directive
  sealed abstract class Declaration extends Directive {
    val name : String
  }
  case class PlainDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration
  case class NodeDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration
  case class EdgeDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration
}

object IR {
  case class Block(val statements : Seq[Statement])

  sealed abstract class Prop
  case class PropBind(val label : String, val bindTo : String) extends Prop
  case class PropExists(val label : String) extends Prop
  case class PropBound(val label : String, val boundTo : String) extends Prop
  case class PropLiteral(val label : String, val literal : Object) extends Prop

  sealed abstract class Rel
  case class RelExistsOut(val label : String) extends Rel
  case class RelExistsIn(val label : String) extends Rel
  case class RelBindOut(val label : String, val bindTo : String) extends Rel
  case class RelBindIn(val label : String, val bindTo : String) extends Rel
  case class RelBoundOut(val label : String, val boundTo : String) extends Rel
  case class RelBoundIn(val label : String, val boundTo : String) extends Rel

  sealed abstract class Statement
  case class NodeQuery(val label : String, val props : Seq[Prop], val rels : Seq[Rel], val bindTo : Option[String]) extends Statement
  case class NodeCheck(val bound : String, val label : String, val props : Seq[Prop], val rels : Seq[Rel]) extends Statement
  case class NodeEnsure(val label : String, val props : Seq[Prop], val rels : Seq[Rel]) extends Statement
  case class EdgeQuery(val label : String, val props : Seq[Prop], val bindLeft : Option[String], val bindRight : Option[String]) extends Statement
  case class EdgeQueryFrom(val label : String, val props : Seq[Prop], val from : String, val bindTo : Option[String]) extends Statement
  case class EdgeQueryTo(val label : String, val props : Seq[Prop], val to : String, val bindFrom : Option[String]) extends Statement
  case class EdgeQueryBoth(val label : String, val props : Seq[Prop], val from : String, val to : String) extends Statement
  case class EdgeEnsure(val label : String, val props : Seq[Prop], val from : String, val to : String) extends Statement
}

object Parser {
  import fastparse._, NoWhitespace._
  import AST._

  def name [_ : P] : P[String] = P( (CharIn("a-zA-Z").! ~ (CharsWhileIn("a-zA-Z0-9").!.?).map({ case None => ""; case Some(s) => s}))
                                    .map(vs => vs._1 ++ vs._2) )
  def wildcard [_ : P] = P( CharsWhileIn("_") ~/ name.? )

  def int [_ : P] : P[Int] = P( (CharIn("0-9").! ~ (CharsWhileIn("0-9").!.?).map({ case None => ""; case Some(s) => s}))
                                .map(vs => Integer.parseInt(vs._1 ++ vs._2)) )
  def intBinding [_ : P] : P[NumberBinding] = P( int.map(i => NumberBinding(i)) )
  def nameBinding [_ : P] : P[NameBinding] = P( name.map(NameBinding(_)) )
  def idBinding [_ : P] : P[IDBinding.type] = P( "$".!.map(_ => IDBinding) )
  def wildcardBinding [_ : P] : P[WildcardBinding.type] = P( wildcard.map(_ => WildcardBinding) )
  def string [_ : P] : P[String] = P( ("\"" ~ ("\\\"" | (!"\"" ~ AnyChar) ).rep.! ~ "\"").map(seq => new String(seq)) )
  def stringBinding [_ : P] : P[SymbolBinding] = P( string.map(str => SymbolBinding(str)) )

  def bodyBind [_ : P] : P[Binding] = P( intBinding | nameBinding | stringBinding | wildcardBinding | idBinding )
  def headBind [_ : P] : P[Binding] = P( intBinding | nameBinding | stringBinding | idBinding )

  def ws [_ : P] = P( Ws.? )
  def Ws [_ : P] = P(
    (
      CharsWhileIn("\r\n\t ") |
      ( "//" ~/ CharsWhile(_ != '\n') )
    ).rep(1) )

  def tpe [_ : P] : P[Type] =
    P( "number".!.map(_ => NumberType) | "symbol".!.map(_ => SymbolType) | "in".!.map(_ => InType) | "out".!.map(_ => OutType) )

  def nameType [_ : P] : P[(String,Type)] = P( name.! ~ ws ~ ":" ~ ws ~ tpe )
  def declaration [_ : P] : P[PlainDeclaration] =
    P(
      (".decl" ~/ Ws ~ name.! ~ ws ~ "(" ~/ ws ~ nameType.rep(sep=(ws ~ "," ~/ ws)) ~ ")").map({
        case (name, types) => PlainDeclaration(name, types)
      }))

  def nodeDecl [_ : P] : P[NodeDeclaration] =
    P(
      (".node" ~/ Ws ~ name.! ~ ws ~ "(" ~/ ws ~ wildcard ~ ws ~ "," ~ ws ~ nameType.rep(sep=(ws ~ "," ~/ ws)) ~ ")").map({
        case (name, _, types) => NodeDeclaration(name, types)
      }))
  def edgeDecl [_ : P] : P[EdgeDeclaration] =
    P(
      (".edge" ~/ Ws ~ name.! ~ ws ~ "(" ~/ ws ~ wildcard ~ ws ~ "," ~ ws ~ wildcard ~ ws ~ (
        "," ~/ ws ~ nameType.rep(sep=(ws ~ "," ~/ ws)) ~ ws).? ~ ")").map({
        case (name, _, _, types) => types match {
          case Some(types) => EdgeDeclaration(name, types)
          case None => EdgeDeclaration(name, Seq.empty)
        }
      }))

  def relation [_ : P] : P[Relation] =
    P( (name.! ~ ws ~ "(" ~/ bodyBind.rep(sep=(ws ~ "," ~/ ws)) ~ ")").map(tpl => Relation(tpl._1, tpl._2)) )

  def condition [_ : P] : P[Condition] = P( relation )

  def fact [_ : P] : P[Fact] =
    P( (name.! ~/ ws ~ "(" ~/ ws ~ headBind.rep(sep=(ws ~ "," ~/ ws)) ~ ws ~ ")" ~ ws ~ (":-" ~ ws ~ condition.rep(sep=(ws ~ "," ~/ ws))).? ~ ws ~ ".")
       .map(tpl => Fact(tpl._1, tpl._2, tpl._3 match { case None => Seq.empty; case Some(s) => s})) )

  def directive [_ : P] : P[Directive] = P( fact | declaration | nodeDecl | edgeDecl )

  def program[_ : P] : P[Seq[Directive]] = P( ws ~ directive.rep(sep=Ws) ~ ws ~ End )

  def tryParse(text : String) : Seq[Directive] =
    parse(text, program(_)) match {
      case Parsed.Success(result, _) => result
      case f : Parsed.Failure => {
        println(f.trace().longAggregateMsg)
        ???
      }
    }
}

object Solver {

  def convertToIR(directives : Seq[AST.Directive]) : Seq[IR.Block] = {
    val decls = new ArrayBuffer[AST.Declaration]
    val facts = new ArrayBuffer[AST.Fact]

    directives.foreach({
      case d : AST.Declaration => decls += d
      case f : AST.Fact => facts += f
    })

    sealed abstract class RelationType
    case object NodeRelation extends RelationType
    case object EdgeRelation extends RelationType
    case object PlainRelation extends RelationType

    val isDefined = new HashSet[String]
    val relationTypes = new HashMap[String,Seq[(String,AST.Type)]]
    val relationType = new HashMap[String,RelationType]

    def checkUniqueNames(names : Seq[String]) : Boolean =
      names.toSet.size == names.length

    decls.foreach(decl => {
      if( isDefined(decl.name) ) {
        ??? // relation is declared twice
      }
      isDefined += decl.name

      decl match {
        case AST.NodeDeclaration(name, types) => {
          if( !checkUniqueNames(types.map(_._1)) ) {
            ??? // names not unique in .decl
          }
          relationType(name) = NodeRelation
          relationTypes(name) = types
        }
        case AST.EdgeDeclaration(name, types) => {
          if( !checkUniqueNames(types.map(_._1)) ) {
            ??? // names not unique in .decl
          }
          relationType(name) = EdgeRelation
          types.foreach({
            case (label, AST.InType) => ??? // edges relations can't relate to nodes
            case (label, AST.OutType) => ??? // other than from and to (which are implicit)
            case _ => ()
          })
          relationTypes(name) = types
        }
        case AST.PlainDeclaration(name, types) => {
          if( !checkUniqueNames(types.map(_._1)) ) {
            ??? // names not unique in .decl
          }
          relationType(name) = PlainRelation
          relationTypes(name) = types
        }
      }
    })


    def getHeadNames(head : Seq[AST.Binding]) : Seq[String] =
      head.flatMap({
        case AST.NumberBinding(_) => Seq.empty
        case AST.SymbolBinding(_) => Seq.empty
        case AST.NameBinding(name) => Seq(name)
        case AST.WildcardBinding => ??? // wildcards invalid in head
        case AST.IDBinding => Seq.empty
      })

    def getBoundBodyNames(body : Seq[AST.Condition]) : Seq[String] =
      body.flatMap({
        case AST.Relation(name, bindings) =>
          bindings.flatMap({
            case AST.NumberBinding(_) => Seq.empty
            case AST.SymbolBinding(_) => Seq.empty
            case AST.NameBinding(name) => Seq(name)
            case AST.WildcardBinding => Seq.empty
            case AST.IDBinding => ??? // ids invalid in body
          })
      }).distinct

    val blocks = facts.map({
      case AST.Fact(name, head, conditions) => {
        if( !isDefined(name) ) {
          ??? // fact must be declared
        }
        
        val headNames = getHeadNames(head)
        val boundBodyNames = getBoundBodyNames(conditions)
        if( !(headNames.toSet &~ boundBodyNames.toSet).isEmpty ) {
          ??? // all head names must be mentioned in the body
        }

        val stmts = new ArrayBuffer[IR.Statement]
        val typeContext = new HashMap[String,AST.Type]
        val isBound = new HashSet[String]

        def handleCondition(cond : AST.Condition) : Unit = cond match {
          case rel : AST.Relation => handleRelationCondition(rel)
        }

        def getBoundProps(bindings : Seq[(AST.Binding,(String,AST.Type))]) : Seq[IR.Prop] =
          bindings.flatMap({
            case (binding, (label, tpe)) =>
              binding match {
                case AST.NumberBinding(num) => Seq(IR.PropLiteral(label, num.asInstanceOf[Object]))
                case AST.SymbolBinding(sym) => Seq(IR.PropLiteral(label, sym))
                case AST.NameBinding(name) => {
                  if( isBound(name) ) {
                    tpe match {
                      case AST.NumberType => Seq(IR.PropBound(label, name))
                      case AST.SymbolType => Seq(IR.PropBound(label, name))
                      case _ => Seq.empty
                    }
                  } else {
                    tpe match {
                      case AST.NumberType => {
                        isBound += name
                        Seq(IR.PropBind(label, name))
                      }
                      case AST.SymbolType => {
                        isBound += name
                        Seq(IR.PropBind(label, name))
                      }
                      case _ => Seq.empty
                    }
                  }
                }
                case AST.WildcardBinding =>
                  tpe match {
                    case AST.NumberType => Seq(IR.PropExists(label))
                    case AST.SymbolType => Seq(IR.PropExists(label))
                    case _ => Seq.empty
                  }
                case AST.IDBinding => ??? // should not be allowed outside of node head
              }
          })
        def getBoundRels(bindings : Seq[(AST.Binding,(String,AST.Type))]) : Seq[IR.Rel] =
          bindings.flatMap({
            case (binding, (label, tpe)) =>
              binding match {
                case AST.NumberBinding(_) => Seq.empty
                case AST.SymbolBinding(_) => Seq.empty
                case AST.NameBinding(name) => {
                  if( isBound(name) ) {
                    tpe match {
                      case AST.NumberType => Seq.empty
                      case AST.SymbolType => Seq.empty
                      case AST.InType => Seq(IR.RelBoundIn(label, name))
                      case AST.OutType => Seq(IR.RelBoundOut(label, name))
                      case AST.SelfType => ??? // unreachable
                    }
                  } else {
                    tpe match {
                      case AST.NumberType => Seq.empty
                      case AST.SymbolType => Seq.empty
                      case AST.InType => {
                        isBound += name
                        Seq(IR.RelBindIn(label, name))
                      }
                      case AST.OutType => {
                        isBound += name
                        Seq(IR.RelBindOut(label, name))
                      }
                      case AST.SelfType => ??? // unreachable
                    }
                  }
                }
                case AST.WildcardBinding =>
                  tpe match {
                    case AST.NumberType => Seq.empty
                    case AST.SymbolType => Seq.empty
                    case AST.InType => Seq(IR.RelExistsIn(label))
                    case AST.OutType => Seq(IR.RelExistsOut(label))
                    case AST.SelfType => ??? // unreachable
                  }
                case AST.IDBinding => ??? // unreachable
              }
          })

        def handleRelationCondition(rel : AST.Relation) : Unit = rel match {
          case AST.Relation(name, bindings) => {
            if( !isDefined(name) ) {
              ??? // used relation must be declared
            }
            val types = relationTypes(name)
            relationType(name) match {
              case NodeRelation => {
                if( types.length + 1 != bindings.length ) {
                  ??? // arity mismatch between declared relation and usage
                }
                unifyTypes(bindings, Seq(AST.SelfType) ++ types.map(_._2))
                val zipped = bindings.tail zip types
                bindings.head match {
                  case AST.NameBinding(self) => {
                    if( isBound(self) ) {
                      stmts += IR.NodeCheck(self, name, getBoundProps(zipped), getBoundRels(zipped))
                    } else {
                      isBound += self
                      stmts += IR.NodeQuery(name, getBoundProps(zipped), getBoundRels(zipped), Some(self))
                    }
                  }
                  case AST.WildcardBinding => {
                    stmts += IR.NodeQuery(name, getBoundProps(zipped), getBoundRels(zipped), None)
                  }
                  case _ => ??? // catastrophic failure, type unification should not allow anything else
                }
              }
              case EdgeRelation => {
                if( types.length + 2 != bindings.length ) {
                  ??? // arity mismatch between declared relation and usage
                }
                unifyTypes(bindings, Seq(AST.InType, AST.OutType) ++ types.map(_._2))
                val zipped = bindings.tail.tail zip types
                (bindings.head, bindings.tail.head) match {
                  case (AST.WildcardBinding, AST.WildcardBinding) => {
                    stmts += IR.EdgeQuery(name, getBoundProps(zipped), None, None)
                  }
                  case (AST.NameBinding(from), AST.WildcardBinding) => {
                    if( isBound(from) ) {
                      stmts += IR.EdgeQueryFrom(name, getBoundProps(zipped), from, None)
                    } else {
                      isBound += from
                      stmts += IR.EdgeQuery(name, getBoundProps(zipped), Some(from), None)
                    }
                  }
                  case (AST.WildcardBinding, AST.NameBinding(to)) => {
                    if( isBound(to) ) {
                      stmts += IR.EdgeQueryTo(name, getBoundProps(zipped), to, None)
                    } else {
                      isBound += to
                      stmts += IR.EdgeQuery(name, getBoundProps(zipped), None, Some(to))
                    }
                  }
                  case (AST.NameBinding(from), AST.NameBinding(to)) => {
                    if( isBound(from) || isBound(to)) {
                      if( isBound(from) && isBound(to) ) {
                        stmts += IR.EdgeQueryBoth(name, getBoundProps(zipped), from, to)
                      } else {
                        if( isBound(from) ) {
                          isBound += to
                          stmts += IR.EdgeQueryFrom(name, getBoundProps(zipped), from, Some(to))
                        } else { // isBound(to)
                          isBound += from
                          stmts += IR.EdgeQueryTo(name, getBoundProps(zipped), to, Some(from))
                        }
                      }
                    } else {
                      isBound += from
                      isBound += to
                      stmts += IR.EdgeQuery(name, getBoundProps(zipped), Some(from), Some(to))
                    }
                  }
                  case _ => ??? // only combinations of wildcards and bindings are expected for
                                // the first two arguments to an edge relation
                }
              }
              case PlainRelation => {
                if( types.length != bindings.length ) {
                  ??? // arity mismatch between declared relation and usage
                }
                unifyTypes(bindings, types.map(_._2))
                val zipped = bindings zip types
                stmts += IR.NodeQuery(name, getBoundProps(zipped), getBoundRels(zipped), None)
              }
            }
          }
        }

        def unifyTypes(head : Seq[AST.Binding], headTypes : Seq[AST.Type]) =
          (head zip headTypes).foreach({
            case (AST.NumberBinding(_), AST.NumberType) => ()
            case (AST.SymbolBinding(_), AST.SymbolType) => ()
            case (AST.NameBinding(name), tpe) => {
              if( typeContext.contains(name) ) {
                (typeContext(name), tpe) match {
                  case (AST.NumberType, AST.NumberType) => ()
                  case (AST.SymbolType, AST.SymbolType) => ()
                  // any combination of in and out is OK
                  // self-type matches both in and out, but not other self
                  // (if we allowed self-self you could ask for 2 vertices to be the same vertex...)
                  case (AST.InType, AST.InType) => ()
                  case (AST.OutType, AST.OutType) => ()
                  case (AST.InType, AST.OutType) => ()
                  case (AST.OutType, AST.InType) => ()
                  case (AST.OutType, AST.SelfType) => ()
                  case (AST.InType, AST.SelfType) => ()
                  case (AST.SelfType, AST.OutType) => ()
                  case (AST.SelfType, AST.InType) => ()
                  case _ => ??? // type mismatch
                }
              } else {
                typeContext(name) = tpe
              }
            }
            case (AST.WildcardBinding, _) => ()
            case (AST.IDBinding, _) => ??? // no ID bindings allowed outside node fact heads
            case _ => ??? // type mismatch with literal
          })

        val headTypes = relationTypes(name)
        relationType(name) match {
          case NodeRelation => {
            if( headTypes.length + 1 != head.length ) {
              ??? // arity mismatch between fact and declared relation
            }
            if( head.head != AST.IDBinding ) {
              ??? // when defining a new node, we can't constrain its identity yet so we require $
            }
            unifyTypes(head.tail, headTypes.map(_._2))
            conditions.foreach(handleCondition(_))
            stmts += IR.NodeEnsure(name, getBoundProps(head.tail zip headTypes), getBoundRels(head.tail zip headTypes))
          }
          case EdgeRelation => {
            if( headTypes.length + 2 != head.length ) {
              ??? // arity mismatch between fact and declared relation
            }
            (head.head, head.tail.head) match {
              case (AST.NameBinding(from), AST.NameBinding(to)) => {
                typeContext(from) = AST.InType
                typeContext(to) = AST.OutType
                unifyTypes(head.tail.tail, headTypes.map(_._2))
                conditions.foreach(handleCondition(_))
                stmts += IR.EdgeEnsure(name, getBoundProps(head.tail.tail zip headTypes), from, to)
              }
              case _ => ??? // when defining a new edge, both from and to must be bound to existing nodes
            }
          }
          case PlainRelation => {
            if( headTypes.length != head.length ) {
              ??? // arity mismatch between fact and declared relation
            }
            unifyTypes(head, headTypes.map(_._2))
            conditions.foreach(handleCondition(_))
            stmts += IR.NodeEnsure(name, getBoundProps(head zip headTypes), getBoundRels(head zip headTypes))
          }
        }

        IR.Block(stmts.toSeq)
      }
    })

    blocks.toSeq
  }

  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversalSource,__,GraphTraversal};
  import org.apache.tinkerpop.gremlin.process.traversal._;
  import org.apache.tinkerpop.gremlin.structure.Edge;
  import org.apache.tinkerpop.gremlin.structure.Vertex;
  import org.apache.tinkerpop.gremlin.structure.io.IoCore;

  def generateTraversal(blocks : Seq[IR.Block], g : GraphTraversalSource) : GraphTraversal[Vertex,Vertex] = {
    import IR._

    var nextFreshVar = 0
    def freshVar : String = {
      val i = nextFreshVar
      nextFreshVar += 1
      s"$$freshVar$i"
    }

    def constrainProp[From,To](src : GraphTraversal[From,To], prop : Prop, shouldUnify : ArrayBuffer[(String,String)]) : GraphTraversal[From,To] = prop match {
      case PropBind(label, bindTo) => src.has(label).as("$tmp").values(label).as(bindTo).select("$tmp")
      case PropExists(label) => src.has(label)
      case PropBound(label, boundTo) => {
        val fresh = freshVar
        val result = src.has(label).as("$tmp").values(label).as(fresh).select("$tmp")
        shouldUnify += ((boundTo, fresh))
        result.asInstanceOf[GraphTraversal[From,To]]
      }
      case PropLiteral(label, literal) => src.has(label, literal)
    }

    def performUnify[From,To](src : GraphTraversal[From,To], shouldUnify : ArrayBuffer[(String,String)]) : GraphTraversal[From,To] = {
      if( !shouldUnify.isEmpty ) {
        val toSelect = shouldUnify.flatMap({
          case (l, r) => Seq(l, r)
        }).distinct.toSeq
        src.filter({
          val selected = __.select(toSelect.head, toSelect.tail.head, toSelect.tail.tail :_*)
          shouldUnify.foldLeft(selected)((src, unif) => unif match {
            case (l, r) => src.where(l, P.eq(r))
          })
        })
      } else {
        src
      }
    }

    def constrainVertex[From](src : GraphTraversal[From,Vertex], props : Seq[Prop], rels : Seq[Rel]) : GraphTraversal[From,Vertex] = {
      val shouldUnify = new ArrayBuffer[(String,String)]

      def constrainRel[From](src : GraphTraversal[From,Vertex], rel : Rel) : GraphTraversal[From,Vertex] = rel match {
        case RelExistsOut(label) => src.outE(label).inV()
        case RelExistsIn(label) => src.inE(label).outV()
        case RelBindOut(label, bindTo) => src.outE(label).as("$tmp").outV().as(bindTo).select("$tmp").inV()
        case RelBindIn(label, bindTo) => src.inE(label).as("$tmp").inV().as(bindTo).select("$tmp").outV()
        case RelBoundOut(label, boundTo) => {
          val fresh = freshVar
          val result = src.as("$tmp").out(label).as(fresh).select("$tmp")
          shouldUnify += ((boundTo, fresh))
          result.asInstanceOf[GraphTraversal[From,Vertex]]
        }
        case RelBoundIn(label, boundTo) => {
          val fresh = freshVar
          val result = src.as("$tmp").in(label).as(fresh).select("$tmp")
          shouldUnify += ((boundTo, fresh))
          result.asInstanceOf[GraphTraversal[From,Vertex]]
        }
      }

      val withProps = props.foldLeft(src)((src, prop) => constrainProp(src, prop, shouldUnify))
      val withRels = rels.foldLeft(withProps)((src, rel) => constrainRel(src, rel))

      performUnify(withRels, shouldUnify)
    }

    def constrainEdge[From](src : GraphTraversal[From,Edge], props : Seq[Prop]) : GraphTraversal[From,Edge] = {
      val shouldUnify = new ArrayBuffer[(String,String)]
      val withProps = props.foldLeft(src)((src, prop) => constrainProp(src, prop, shouldUnify))
      performUnify(withProps, shouldUnify)
    }

    def ensureProps[From,To](src : GraphTraversal[From,To], props : Seq[Prop]) : GraphTraversal[From,To] = {
      def ensureProp[From,To](src : GraphTraversal[From,To], prop : Prop) : GraphTraversal[From,To] = prop match {
        case PropBind(label, bindTo) => ???
        case PropExists(label) => ???
        case PropBound(label, boundTo) => src.property(label, __.select(boundTo))
        case PropLiteral(label, literal) => src.property(label, literal)
      }
      val withProps = props.foldLeft(src)((src, prop) => ensureProp(src, prop))
      withProps
    }

    def ensurePropsRels[From](src : GraphTraversal[From,Vertex], props : Seq[Prop], rels : Seq[Rel]) : GraphTraversal[From,Vertex] = {
      def ensureRel[From,To](src : GraphTraversal[From,Vertex], rel : Rel) : GraphTraversal[From,Vertex] = rel match {
        case RelExistsOut(label) => ???
        case RelExistsIn(label) => ???
        case RelBindOut(label, bindTo) => ???
        case RelBindIn(label, bindTo) => ???
        case RelBoundOut(label, boundTo) => src.addE(label).to(boundTo).outV()//.sideEffect((t : Traverser[Vertex]) => println(s"OUT $t"))
        case RelBoundIn(label, boundTo) => src.addE(label).from(boundTo).inV()//.sideEffect((t : Traverser[Vertex]) => println(s"IN $t"))
      }
      val withProps = ensureProps(src, props)
      val withRels = rels.foldLeft(withProps)((src, rel) => ensureRel(src, rel))
      withRels
    }

    def genNextStatement[From](src : GraphTraversal[From,Vertex], stmt : Statement) : GraphTraversal[From,Vertex] = stmt match {
      case NodeQuery(label, props, rels, bindTo) => {
        val q = constrainVertex(src.V().hasLabel(label), props, rels)
        bindTo match {
          case Some(name) => q.as(name)
          case None => q
        }
      }
      case NodeCheck(bound, label, props, rels) =>
        constrainVertex(src.select(bound), props, rels).as(bound)
      case NodeEnsure(label, props, rels) => {
        val names = props.flatMap({
          case PropBind(label, bindTo) => ???
          case PropExists(label) => ???
          case PropBound(label, boundTo) => List(boundTo)
          case PropLiteral(label, literal) => List.empty
        }) ++ rels.flatMap({
          case RelExistsOut(label) => ???
          case RelExistsIn(label) => ???
          case RelBindOut(label, bindTo) => ???
          case RelBindIn(label, bindTo) => ???
          case RelBoundOut(label, boundTo) => List(boundTo)
          case RelBoundIn(label, boundTo) => List(boundTo)
        })
        val q = {
          if( names.length > 1 ) {
            src.select(names.head, names.tail.head, names.tail.tail :_*)//.sideEffect((t : Traverser[java.util.Map[String,Any]]) => println(s"S $t"))
          } else if( !names.isEmpty ) {
            src.select(names.head)
          } else { 
            src
          }
        }.where(constrainVertex(__.V().hasLabel(label), props, rels)/*.sideEffect((t : Traverser[Vertex]) => println(s"N $t"))*/.count().is(0))
        ensurePropsRels(q.addV(label), props, rels)
      }
      case EdgeQuery(label, props, bindFrom, bindTo) => {
        val q = constrainEdge(src.V().outE(label), props)
        val boundFrom = bindFrom match {
          case Some(name) => q.as("$tmp").inV().as(name).select("$tmp")
          case None => q
        }
        bindTo match {
          case Some(name) => q.outV().as(name)
          case None => q.outV()
        }
      }
      case EdgeQueryFrom(label, props, from, bindTo) => {
        val q = constrainEdge(src.select(from).outE(label), props)
        bindTo match {
          case Some(name) => q.outV().as(name)
          case None => q.outV()
        }
      }
      case EdgeQueryTo(label, props, to, bindFrom) => {
        val q = constrainEdge(src.select(to).inE(label), props)
        bindFrom match {
          case Some(name) => q.inV().as(name)
          case None => q.inV()
        }
      }
      case EdgeQueryBoth(label, props, from, to) =>
        constrainEdge(src.select(from).outE(label), props).outV().where(P.eq(to))
      case EdgeEnsure(label, props, from, to) => {
        val q = src.where(constrainEdge(__.V().outE(label), props).count().is(0))
        ensureProps(q.addE(label).from(from).to(to), props).outV()
      }
    }

    def genBlockTraversal(block : Block) : GraphTraversal[Vertex,Vertex] = block match {
      case Block(stmts) => {
        if( stmts.isEmpty ) {
          ??? // how?
        }
        stmts.tail.foldLeft(genNextStatement(__.identity(), stmts.head))((src, stmt) => {
          genNextStatement(src, stmt)
        })
      }
    }

    g.V().hasLabel("$$$DUMMY$$$").fold().coalesce(__.unfold(), __.addV("$$$DUMMY$$$")).repeat(
      __.union(
        blocks.map(genBlockTraversal(_)) :_*
      )
    ).emit()
  }
}


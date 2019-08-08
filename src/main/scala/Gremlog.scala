package ca.uwaterloo.gremlog

import collection.mutable.{HashSet,HashMap,ArrayBuffer,ArrayDeque,TreeSet,TreeMap}

object AST {
  sealed abstract class Type
  case object NumberType extends Type
  case object SymbolType extends Type
  case object VertexType extends Type
  case object EdgeType extends Type

  sealed abstract class Binding
  case class SymbolBinding(val value : String) extends Binding
  case class NumberBinding(val value : Int) extends Binding
  case class NameBinding(val name : String) extends Binding
  case object WildcardBinding extends Binding
  case object AnonymousIDBinding extends Binding
  case class NamedIDBinding(val name : String) extends Binding

  sealed abstract class InfixRelationKind
  case object InfixRelationEQ extends InfixRelationKind
  case object InfixRelationNEQ extends InfixRelationKind
  case object InfixRelationLT extends InfixRelationKind
  case object InfixRelationGT extends InfixRelationKind
  case object InfixRelationLTE extends InfixRelationKind
  case object InfixRelationGTE extends InfixRelationKind

  sealed abstract class ExpressionBinaryKind
  case object ExpressionBinaryPlus extends ExpressionBinaryKind
  case object ExpressionBinaryMinus extends ExpressionBinaryKind

  sealed abstract class Expression
  case class ExpressionName(val name : String) extends Expression
  case class ExpressionProp(val base : String, val prop : String) extends Expression
  case class ExpressionNumber(val value : Int) extends Expression
  case class ExpressionSymbol(val value : String) extends Expression
  case class ExpressionBinary(val kind : ExpressionBinaryKind, val lhs : Expression, val rhs : Expression) extends Expression

  sealed abstract class Condition
  case class Relation(val name : String, val bindings : Seq[Binding]) extends Condition
  case class Negation(val relation : Relation) extends Condition
  case class InfixRelation(val kind : InfixRelationKind, val lhs : Expression, val rhs : Expression) extends Condition

  sealed abstract class Directive
  case class Fact(val name : String, val head : Seq[Binding], val conditions : Seq[Condition]) extends Directive
  case class OutputDirective(val name : String) extends Directive

  sealed abstract class Declaration extends Directive {
    val name : String
  }
  case class PlainDeclaration(val name : String, val types : Seq[(String,Type)]) extends Declaration
  case class VertexDeclaration(val name : String) extends Declaration
  case class EdgeDeclaration(val name : String) extends Declaration
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
  def anonIDBinding [_ : P] : P[AnonymousIDBinding.type] = P( "$".!.map(_ => AnonymousIDBinding) )
  def namedIDBinding [_ : P] : P[NamedIDBinding] = P( "$" ~ ws ~ ":" ~ name.map(n => NamedIDBinding(n)) )
  def wildcardBinding [_ : P] : P[WildcardBinding.type] = P( wildcard.map(_ => WildcardBinding) )
  def string [_ : P] : P[String] = P( ("\"" ~ ("\\\"" | (!"\"" ~ AnyChar) ).rep.! ~ "\"").map(seq => new String(seq)) )
  def stringBinding [_ : P] : P[SymbolBinding] = P( string.map(str => SymbolBinding(str)) )

  def bodyBind [_ : P] : P[Binding] = P( intBinding | nameBinding | stringBinding | wildcardBinding )
  def headBind [_ : P] : P[Binding] = P( intBinding | nameBinding | stringBinding | namedIDBinding | anonIDBinding )

  def ws [_ : P] = P( Ws.? )
  def Ws [_ : P] = P(
    (
      CharsWhileIn("\r\n\t ") |
      ( "//" ~/ CharsWhile(_ != '\n') )
    ).rep(1) )

  def tpe [_ : P] : P[Type] =
    P( "number".!.map(_ => NumberType) | "symbol".!.map(_ => SymbolType) | "vertex".!.map(_ => VertexType) )

  def nameType [_ : P] : P[(String,Type)] = P( name.! ~ ws ~ ":" ~ ws ~ tpe )
  def plainDecl [_ : P] : P[PlainDeclaration] =
    P(
      (".decl" ~/ Ws ~ name.! ~ ws ~ "(" ~/ ws ~ nameType.rep(sep=(ws ~ "," ~/ ws)) ~ ")").map({
        case (name, types) => PlainDeclaration(name, types)
      }))

  def vertexDecl [_ : P] : P[VertexDeclaration] =
    P( (".node" ~/ Ws ~ name.! ~ Ws).map(name => VertexDeclaration(name)) )
  def edgeDecl [_ : P] : P[EdgeDeclaration] =
    P( (".edge" ~/ Ws ~ name.! ~ Ws).map(name => EdgeDeclaration(name)) )

  def relation [_ : P] : P[Relation] =
    P( (name.! ~ ws ~ "(" ~/ bodyBind.rep(sep=(ws ~ "," ~/ ws)) ~ ")").map(tpl => Relation(tpl._1, tpl._2)) )
  def negation [_ : P] : P[Negation] =
    P( "!" ~ ws ~ relation.map(r => Negation(r)) )

  def expression [_ : P] : P[Expression] =
    P(
      name.!.map(name => ExpressionName(name)) |
      ( name.! ~ ws ~ "." ~ ws ~ name.! ).map({ case (base, prop) => ExpressionProp(base, prop) }) |
      int.map(n => ExpressionNumber(n)) |
      string.map(str => ExpressionSymbol(str)) )

  def infixRelation [_ : P] : P[InfixRelation] =
    P(
      (
        expression ~ ws ~
        (
          "=".!.map(_ => InfixRelationEQ) |
          "\\=".!.map(_ => InfixRelationNEQ) |
          "<".!.map(_ => InfixRelationLT) |
          ">".!.map(_ => InfixRelationGT) |
          "<=".!.map(_ => InfixRelationLTE) |
          ">=".!.map(_ => InfixRelationGTE) ) ~/
        ws ~ expression
      ).map({
        case (lhs, kind, rhs) => InfixRelation(kind, lhs, rhs)
      }) )

  def condition [_ : P] : P[Condition] = P( relation | negation | infixRelation )

  def fact [_ : P] : P[Fact] =
    P( (name.! ~/ ws ~ "(" ~/ ws ~ headBind.rep(sep=(ws ~ "," ~/ ws)) ~ ws ~ ")" ~ ws ~ (":-" ~ ws ~ condition.rep(sep=(ws ~ "," ~/ ws))).? ~ ws ~ ".")
       .map(tpl => Fact(tpl._1, tpl._2, tpl._3 match { case None => Seq.empty; case Some(s) => s})) )

  def outputDirective [_ : P] : P[OutputDirective] =
    P( ".output" ~/ Ws ~ name.!.map(OutputDirective(_)) )

  def directive [_ : P] : P[Directive] = P( fact | outputDirective | plainDecl | vertexDecl | edgeDecl )

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

// map all names to (relation name, fact idx, name)
// type check vertex / edge / number / symbol
// map each fact to a Cypher query, stratified by NOT (check dependencies)
// for each conjunction, generally translate a condition to a MATCH / WHERE. try to group properties with node matches where possible (Cypher perf thing)
// end with one or more MERGE expressions for all the productions

// for each fact, run concurrently as a transaction. as long as rows are created, keep rerunning.
// batch by stratified layer
// perf concern: transitive closure might be slow if we do this?

// for incremental:
// select all the HEAD clauses, WHERE NOT any clause; DELETE HEAD if any pass

object Compiler {

  def compile(directives : Seq[AST.Directive]) : Seq[Seq[String]] = {
    val decls = new ArrayBuffer[AST.Declaration]()
    val facts = new ArrayBuffer[AST.Fact]()
    val outputs = new TreeSet[String]()
    directives.foreach({
      case d : AST.Declaration => decls += d
      case f : AST.Fact => facts += f
      case AST.OutputDirective(name) => {
        if( outputs(name) ) {
          ??? // cannot state that the same fact should be output twice
        }
        outputs += name
      }
    })

    val declTypes = new HashMap[String,Seq[(String,AST.Type)]]()
    val isVertex = new HashSet[String]()
    val isEdge = new HashSet[String]()
    for(decl <- decls) {
      if(declTypes.contains(decl.name) || isVertex(decl.name) || isEdge(decl.name)) {
        ??? // cannot declare the same fact twice
      }
      decl match {
        case AST.PlainDeclaration(name, types) => declTypes += ((name, types))
        case AST.VertexDeclaration(name) => isVertex += name
        case AST.EdgeDeclaration(name) => isEdge += name
      }
    }

    val factTree = new TreeMap[String,ArrayBuffer[(Seq[AST.Binding],Seq[AST.Condition])]]()

    facts.foreach({
      case AST.Fact(name, head, conditions) => {
        if( !declTypes.contains(name) ) {
          ??? // IDB fact requires type declaration (TODO: allow multiple heads?)
        }
        // TODO: typecheck here
        val cases = factTree.getOrElseUpdate(name, new ArrayBuffer[(Seq[AST.Binding],Seq[AST.Condition])]())
        cases += ((head, conditions)) 
      }
    })

    val evaluationOrderGroupCandidates = new ArrayBuffer[ArrayBuffer[String]]()

    def findEvaluationOrderCandidates(head : String, evaluationOrderCandidates : ArrayBuffer[String], visited : Set[String], negatedIn : Set[String]) : Unit = {
      if(negatedIn(head)) {
        ??? // violates stratification - we are inside a negation, but reference the thing negating us
      }

      if(!visited(head) && factTree.contains(head)) {
        factTree(head).foreach({
          case (bindings, conditions) =>
            conditions.foreach({
              case AST.Relation(name, _) => {
                findEvaluationOrderCandidates(name, evaluationOrderCandidates, visited + head, negatedIn)
              }
              case AST.Negation(AST.Relation(name, _)) => {
                val groupCandidate = new ArrayBuffer[String]()
                findEvaluationOrderCandidates(name, groupCandidate, visited + head, negatedIn + head)
                evaluationOrderGroupCandidates += groupCandidate
              }
              case AST.InfixRelation(_, _, _) => ()
            })
        })
        evaluationOrderCandidates += head
      }
    }

    val evaluationOrderCandidatesTopLevel = new ArrayBuffer[String]()
    for(output <- outputs) {
      findEvaluationOrderCandidates(output, evaluationOrderCandidatesTopLevel, Set.empty, Set.empty)
    }
    evaluationOrderGroupCandidates += evaluationOrderCandidatesTopLevel

    val evalSet = new HashSet[String]()
    val evaluationOrderGroups = evaluationOrderGroupCandidates.map(_.filter(name => {
      if(evalSet(name)) {
        false
      } else {
        evalSet += name
        true
      }
    }))

    var freshCounter = 0
    def freshName : String = {
      freshCounter += 1
      s"freshName$freshCounter"
    }

    def printLiteralBinding(bind : AST.Binding) : String = bind match {
      case AST.SymbolBinding(v) => s""""${v}""""
      case AST.NumberBinding(v) => s"$v"
      case _ => ??? // that's not a literal
    }

    def printExpression(expr : AST.Expression, propBinds : HashMap[String,AST.Expression]) : String = expr match {
      case AST.ExpressionName(name) if propBinds.contains(name) => printExpression(propBinds(name), propBinds)
      case AST.ExpressionName(name) => s"`$name`" // this has to be comparing node identity or edge identity (or it would not typecheck)
      case AST.ExpressionProp(base, prop) => s"`$base`.`$prop`"
      case AST.ExpressionNumber(v) => s"$v"
      case AST.ExpressionSymbol(v) => s""""$v""""
      case AST.ExpressionBinary(kind, lhs, rhs) => {
        val op = kind match {
          case AST.ExpressionBinaryPlus => "+"
          case AST.ExpressionBinaryMinus => "-"
        }
        s"${printExpression(lhs, propBinds)} $op ${printExpression(rhs, propBinds)}"
      }
    }

    def genRelationCheck(rel : AST.Relation, propBinds : HashMap[String,AST.Expression], self : String = freshName) : String = rel match {
      case AST.Relation(name, bindings) => {
        if( declTypes.contains(name) ) {
          val vertices = new ArrayBuffer[(String,AST.Binding)]()
          val props = new ArrayBuffer[(String,AST.Binding)]()
          (bindings zip declTypes(name)).foreach({
            case (bind, (n, AST.VertexType)) => vertices += ((n, bind))
            case (bind, (n, _)) => props += ((n, bind))
          })

          val binds = // if propBinds is specified, we should generate a pattern with all the EQ information embedded (for inside e.g a NOT)
            if( propBinds != null ) {
              props.flatMap({
                case (pName, AST.NameBinding(boundName)) => Seq(s"`$pName`: ${printExpression(propBinds(boundName), propBinds)}")
                case (_, AST.WildcardBinding) => Seq.empty // TODO: check for existence
                case (pName, bind) => Seq(s"`$pName`: ${printLiteralBinding(bind)}")
              })
            } else {
              props.flatMap({
                case (pName, AST.NameBinding(_)) => Seq.empty // these get converted into equality checks, ignore here as bindings are not given
                case (_, AST.WildcardBinding) => Seq.empty // TODO: check for existence
                case (pName, bind) => Seq(s"`$pName`: ${printLiteralBinding(bind)}")
              })
            }
          val withProps =
            if(!binds.isEmpty) {
              s"(`$self`:`$name` { ${binds.mkString(", ")} })"
            } else {
              s"(`$self`:`$name`)"
            }
          if(!vertices.isEmpty) {
            val links = vertices.flatMap({
              case (eName, AST.NameBinding(otherVertex)) => Seq(s"(`$self`)-[:`$eName`]->(`$otherVertex`)")
              case (eName, AST.WildcardBinding) => Seq.empty
              case _ => ??? // putting anything else for a vertex is not type-correct
            })
            s"$withProps, ${links.mkString(", ")}"
          } else {
            withProps
          }
        } else if( isVertex(name) ) {
          Predef.assert(bindings.length == 1)
          bindings.head match {
            case AST.NameBinding(boundName) => s"(`$boundName`:`$name`)"
            case AST.WildcardBinding => s"(:`$name`)" // TODO: does this even makes sense?
            case _ => ??? // no other case is type-correct here
          }
        } else if( isEdge(name) ) {
          Predef.assert(bindings.length == 3)
          val Seq(edge, l, r) = bindings.map({
            case AST.NameBinding(boundName) => s"`$boundName`"
            case AST.WildcardBinding => ""
            case _ => ??? // no other case is type-correct here
          })
          // backticks are inserted above dring the mapping, there weren't forgotten
          s"($l)-[$edge:$name]->($r)"
        } else {
          ??? // type error
        }
      }
    }
    
    evaluationOrderGroups.map(_.flatMap(head => {
      factTree(head).map({
        case (bindings, conditions) => {
          val patternParts = new ArrayBuffer[(String,AST.Relation)]()
          val patternChecks = new ArrayBuffer[AST.Condition]()
          val propBinds = HashMap[String,AST.Expression]()
          conditions.foreach({
            case r @ AST.Relation(rName, rBindings) => {
              val vertexName = freshName
              patternParts += ((vertexName, r))
              (rBindings zip declTypes(rName)).foreach({
                case (_, (_, AST.VertexType)) => ()
                case (AST.NameBinding(boundName), (prop, _)) => {
                  if( propBinds.contains(boundName) ) {
                    patternChecks += AST.InfixRelation(AST.InfixRelationEQ, propBinds(boundName), AST.ExpressionProp(vertexName,prop))
                  } else {
                    propBinds += ((boundName, AST.ExpressionProp(vertexName,prop)))
                  }
                }
                case _ => () // non-name bindings get picked up later
              })
            }
            case n : AST.Negation => patternChecks += n
            case i : AST.InfixRelation => patternChecks += i
          })

          val matchParts = patternParts.map({
            case (self, rel) => genRelationCheck(rel, propBinds=null, self=self)
          })
          
          val checkParts = patternChecks.map({
            case _ : AST.Relation => ??? // unreachable
            case AST.Negation(rel) =>
              s"(NOT ${genRelationCheck(rel, propBinds=propBinds)})"
            case AST.InfixRelation(kind, lhs, rhs) => {
              val op = kind match {
                case AST.InfixRelationEQ => "="
                case AST.InfixRelationNEQ => "<>"
                case AST.InfixRelationLT => "<"
                case AST.InfixRelationGT => ">"
                case AST.InfixRelationLTE => "<="
                case AST.InfixRelationGTE => ">="
              }
              s"(${printExpression(lhs, propBinds)} $op ${printExpression(rhs, propBinds)})"
            }
          })
          
          val mergeProps = new ArrayBuffer[(AST.Binding,String)]()
          val mergeVertices = new ArrayBuffer[(AST.Binding,String)]()
          (bindings zip declTypes(head)).foreach({
            case (bind, (name, AST.VertexType)) => mergeVertices += ((bind, name))
            case (bind, (name, _)) => mergeProps += ((bind, name))
          })

          val self = freshName

          val propParts = mergeProps.map({
            case (AST.NameBinding(boundName), propName) => s"`$propName`: ${printExpression(propBinds(boundName), propBinds)}"
            case (bind, propName) => s"`$propName`: ${printLiteralBinding(bind)}"
          })
          val vertexParts = mergeVertices.map({
            case (AST.NameBinding(boundName), edgeType) => s"MERGE (`$self`)-[:`$edgeType`]->(`${boundName}`)"
            case _ => ??? // TODO: identity bindings?; otherwise, no other binding makes sense in HEAD
          })

          val result1 = if( !matchParts.isEmpty ) s"""MATCH ${matchParts.mkString(", ")}\n""" else ""
          val result2 = if( !checkParts.isEmpty ) s"WHERE ${checkParts.mkString(" AND ")}\n" else ""

          result1 ++ result2 ++
          s"MERGE (`$self`:`$head` { ${propParts.mkString(", ")} })" ++
          vertexParts.mkString("\n")
        }
      })
    }).toSeq).toSeq
  }

}


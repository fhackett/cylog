package ca.uwaterloo.cylog

import scala.util.{Using,Try}
import scala.collection.mutable.{HashSet,HashMap,ArrayBuffer,ArrayDeque,TreeSet,TreeMap}

object AST {
  sealed abstract class Type
  case object NumberType extends Type
  case object SymbolType extends Type
  case object VertexType extends Type
  case object EdgeType extends Type

  sealed abstract class Binding
  //case class SymbolBinding(val value : String) extends Binding
  //case class NumberBinding(val value : Int) extends Binding
  //case class NameBinding(val name : String) extends Binding
  case class ExprBinding(val expr : Expression) extends Binding
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
  case object ExpressionBinaryTimes extends ExpressionBinaryKind
  case object ExpressionBinaryDivide extends ExpressionBinaryKind

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
  //def intBinding [_ : P] : P[NumberBinding] = P( int.map(i => NumberBinding(i)) )
  //def nameBinding [_ : P] : P[NameBinding] = P( name.map(NameBinding(_)) )
  def anonIDBinding [_ : P] : P[AnonymousIDBinding.type] = P( "$".!.map(_ => AnonymousIDBinding) )
  def namedIDBinding [_ : P] : P[NamedIDBinding] = P( "$" ~ ws ~ ":" ~ name.map(n => NamedIDBinding(n)) )
  def wildcardBinding [_ : P] : P[WildcardBinding.type] = P( wildcard.map(_ => WildcardBinding) )
  def string [_ : P] : P[String] = P( ("\"" ~ ("\\\"" | (!"\"" ~ AnyChar) ).rep.! ~ "\"").map(seq => new String(seq)) )
  //def stringBinding [_ : P] : P[SymbolBinding] = P( string.map(str => SymbolBinding(str)) )
  def exprBinding [_ : P] : P[ExprBinding] = P( expression.map(expr => ExprBinding(expr)) )

  def bodyBind [_ : P] : P[Binding] = P( exprBinding | wildcardBinding )
  def headBind [_ : P] : P[Binding] = P( exprBinding | namedIDBinding | anonIDBinding )

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
    P( (".node" ~/ Ws ~ name.!).map(name => VertexDeclaration(name)) )
  def edgeDecl [_ : P] : P[EdgeDeclaration] =
    P( (".edge" ~/ Ws ~ name.!).map(name => EdgeDeclaration(name)) )

  def relation [_ : P] : P[Relation] =
    P( (name.! ~ ws ~ "(" ~/ bodyBind.rep(sep=(ws ~ "," ~/ ws)) ~ ")").map(tpl => Relation(tpl._1, tpl._2)) )
  def negation [_ : P] : P[Negation] =
    P( "!" ~ ws ~ relation.map(r => Negation(r)) )

  def expressionBase [_ : P] : P[Expression] =
    P(
      ( "(" ~/ expression ~ ")" ) |
      ( name.! ~ ws ~ "." ~ ws ~ name.! ).map({ case (base, prop) => ExpressionProp(base, prop) }) |
      name.!.map(name => ExpressionName(name)) |
      int.map(n => ExpressionNumber(n)) |
      string.map(str => ExpressionSymbol(str)) )

  def expressionAddSub [_ : P] : P[Expression] =
    P(
      (expressionBase ~ ws ~ (("+".! | "-".!) ~/ ws ~ expressionBase).rep(sep=ws)).map({
        case (lhs, rhss) =>
          rhss.foldLeft(lhs)({
            case (lhs, (op, rhs)) => ExpressionBinary(
              op match {
                case "+" => ExpressionBinaryPlus
                case "-" => ExpressionBinaryMinus
              },
              lhs,
              rhs)
          })
      }) )

  def expressionMulDiv [_ : P] : P[Expression] =
    P(
      (expressionAddSub ~ ws ~ (("*".! | "/".!) ~/ ws ~ expressionAddSub).rep(sep=ws)).map({
        case (lhs, rhss) =>
          rhss.foldLeft(lhs)({
            case (lhs, (op, rhs)) => ExpressionBinary(
              op match {
                case "*" => ExpressionBinaryTimes
                case "/" => ExpressionBinaryDivide
              },
              lhs,
              rhs)
          })
      }) )
  
  def expression [_ : P] : P[Expression] = expressionMulDiv

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

  def compile(directives : Seq[AST.Directive]) : Seq[Seq[(String,Seq[String],String)]] = {
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

    val evaluationOrderGroups = {
      // unique over all groups, .unique is not correct here
      val uniqSet = new HashSet[String]()
      evaluationOrderGroupCandidates.map(_.filter(candidate =>
        if( !uniqSet(candidate) ) {
          uniqSet += candidate
          true
        } else {
          false
        }))
    }

    var freshCounter = 0
    def freshName : String = {
      freshCounter += 1
      s"freshName$freshCounter"
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
        s"(${printExpression(lhs, propBinds)} $op ${printExpression(rhs, propBinds)})"
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
                case (pName, AST.ExprBinding(expr)) => Seq(s"`$pName`: ${printExpression(expr, propBinds)}")
                case (_, AST.WildcardBinding) => Seq.empty // TODO: check for existence
              })
            } else {
              props.flatMap({
                case (pName, AST.ExprBinding(_)) => Seq.empty // these get converted into equality checks, ignore here as bindings are not given
                case (_, AST.WildcardBinding) => Seq.empty // TODO: check for existence
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
              case (eName, AST.ExprBinding(AST.ExpressionName(otherVertex))) => Seq(s"(`$self`)-[:`$name$$$eName`]->(`$otherVertex`)")
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
            case AST.ExprBinding(AST.ExpressionName(boundName)) => s"(`$boundName`:`$name`)"
            case AST.WildcardBinding => s"(:`$name`)" // TODO: does this even makes sense?
            case _ => ??? // no other case is type-correct here
          }
        } else if( isEdge(name) ) {
          Predef.assert(bindings.length == 3)
          val Seq(edge, l, r) = bindings.map({
            case AST.ExprBinding(AST.ExpressionName(boundName)) => s"`$boundName`"
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
          val dependsOn = new HashSet[String]()
          val dependsOnSeq = new ArrayBuffer[String]()
          val patternParts = new ArrayBuffer[(String,AST.Relation)]()
          val patternChecks = new ArrayBuffer[AST.Condition]()
          val propBinds = HashMap[String,AST.Expression]()
          conditions.foreach({
            case r @ AST.Relation(rName, rBindings) => {
              if( !dependsOn(rName) ) {
                dependsOn += rName
                dependsOnSeq += rName
              }

              val vertexName = freshName
              patternParts += ((vertexName, r))
              // if rName is not in declTypes, is has to be a vertex or edge
              if( declTypes.contains(rName) ) {
                (rBindings zip declTypes(rName)).foreach({
                  case (_, (_, AST.VertexType)) => ()
                  case (AST.ExprBinding(AST.ExpressionName(boundName)), (prop, _)) => {
                    if( propBinds.contains(boundName) ) {
                      patternChecks += AST.InfixRelation(AST.InfixRelationEQ, propBinds(boundName), AST.ExpressionProp(vertexName,prop))
                    } else {
                      propBinds += ((boundName, AST.ExpressionProp(vertexName,prop)))
                    }
                  }
                  case (AST.ExprBinding(expr), (prop, _)) => {
                    patternChecks += AST.InfixRelation(AST.InfixRelationEQ, AST.ExpressionProp(vertexName, prop), expr)
                  }
                  case (AST.WildcardBinding, _) => () // wildcards bind nothing, TODO: make wildcards check for existence
                })
              } else {
                assert( isVertex(rName) || isEdge(rName) )
              }
            }
            case n : AST.Negation => {
              if( !dependsOn(n.relation.name) ) {
                dependsOn += n.relation.name
                dependsOnSeq += n.relation.name
              }
              patternChecks += n
            }
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
            case (AST.ExprBinding(expr), propName) => s"`$propName`: ${printExpression(expr, propBinds)}"
            case _ => ??? // you can bind a property to any expression. non-expressions make no sense
          })

          val result1 = if( !matchParts.isEmpty ) s"""MATCH ${matchParts.mkString(", ")}\n""" else ""
          val result2 = if( !checkParts.isEmpty ) s"WHERE ${checkParts.mkString(" AND ")}\n" else ""

          val cypherBody = result1 ++ result2 ++
            // for vertices, make sure the initial merge on $self isn't just the node, or else there will only be exactly one node of type
            // $edgeType ever, since ($self:$edgeType) will match anything of $edgeType
            s"MERGE (`$self`:`$head` { ${propParts.mkString(", ")} })" ++
              (mergeVertices match {
                case scala.collection.mutable.Seq() => "\n"
                case scala.collection.mutable.Seq(first, rest @ _*) => {
                  val createChecks = new ArrayBuffer[String]()
                  createChecks += self

                  val r1 = freshName
                  createChecks += r1
                  val firstResult = (first match {
                    case (AST.ExprBinding(AST.ExpressionName(boundName)), edgeType) => s"-[`$r1`:`$head$$$edgeType`]->(`${boundName}`)\n"
                    case _ => ??? // see below for identity bindings
                  }) ++ s"ON CREATE SET `$self`.`$$created` = true, `$r1`.`$$created` = true\n"
                  firstResult ++ rest.map({
                    case (AST.ExprBinding(AST.ExpressionName(boundName)), edgeType) => {
                      val relName = freshName
                      createChecks += relName

                      s"MERGE (`$self`)-[`$relName`:`$head$$$edgeType`]->(`${boundName}`)\n" ++
                      s"ON CREATE SET `$relName`.`$$created` = true\n"
                    }
                    case _ => ??? // TODO: identity bindings?; otherwise, no other binding makes sense in HEAD
                  }).mkString ++
                  s"WITH ${createChecks.map(s => s"`$s`").mkString(", ")}\n" ++
                  s"WHERE ${createChecks.map(s => s"`$s`.`$$created`").mkString(" OR ")}\n" ++
                  s"WITH ${createChecks.map(s => s"`$s`").mkString(", ")} LIMIT 500000\n" ++
                  s"REMOVE ${createChecks.map(s => s"`$s`.`$$created`").mkString(", ")}\n"
                }
              })
          
          (head, dependsOnSeq.toSeq, cypherBody)
        }
      })
    }).toSeq).toSeq
  }

}

object Runner {
  import org.neo4j.driver.v1.{AuthTokens,Driver,GraphDatabase,Session,StatementResult,TransactionConfig}

  def run(blocks : Seq[Seq[(String,Seq[String],String)]], session : Session) : Unit = { 
    import Main.prettyTime

    val queryStart = System.nanoTime()
    try {
      for(block <- blocks) {
        // this section of code computes a flipped dependency relation between Datalog facts.
        // if one Datalog fact uses some set of others, this algorithm computes, given new instances of some relation F,
        // all relations F' that need recomputing based on it.
        // Consider especially: if a relation has two cases F1 and F2, and only F1 depends on a relation G, only F1 will be computed when
        // G changes.
        val headIds : Map[String,ArrayBuffer[Int]] = locally {
          val headIds = new HashMap[String,(HashSet[Int],ArrayBuffer[Int])]()
          for(((head, dependsOn, cypher), id) <- block.zipWithIndex) {
            val (idSet, ids) = headIds.getOrElseUpdate(head, (new HashSet[Int](), new ArrayBuffer[Int]()))
            if( !idSet(id) ) {
              ids += id
              idSet += id
            }
          }
          headIds.map({ case (head, (idSet, ids)) => (head, ids) }).toMap
        }

        val todo = new ArrayDeque[Int]()

        val cypherMap = new HashMap[Int,String]()
        val affects = new HashMap[Int,TreeSet[Int]]()
        val nameOf = new HashMap[Int,String]()
        val indexOfName = new HashMap[String,Int]()
        for(((head, dependsOn, cypher), id) <- block.zipWithIndex) {
          todo += id
          cypherMap(id) = cypher
          // generate names that make sense in context of the Datalog (i.e idx 0 is first definition, 1 is second, etc...)
          val idx = indexOfName.getOrElseUpdate(head, 0)
          indexOfName(head) += 1
          nameOf(id) = s"$head@$idx"

          for(depName <- dependsOn if headIds.contains(depName); dep <- headIds(depName)) {
            val affectsSet = affects.getOrElseUpdate(dep, new TreeSet[Int]())
            affectsSet += id
          }
        }

        //println(s"Affects: ${affects.map({ case (from, to) => s"${nameOf(from)}: ${to.map(nameOf).mkString(", ")}"}).mkString("\n- ")}")

        val isTodo = new HashSet[Int]()
        isTodo ++= todo

        // Logic: if a relation produced changes in the DB and has dependencies, append all its dependencies to the todo queue,
        // but only if they aren't already there (shown by isTodo). Continue until fixpoint is reached, that is, the entire remaining
        // queue produces no DB changes, scheduling no more relations.
        while(!todo.isEmpty) {
          val id = todo.removeHead()
          isTodo -= id
          val cypher = cypherMap(id)

          var shouldSchedule = false
          var wasProductive = false

          println(s"Running fragment ${nameOf(id)}...")

          val fragmentStart = System.nanoTime()
          do {
            val blockStart = System.nanoTime()
            val result = session.run(cypher, TransactionConfig.empty())
            val summary = result.consume()
            val blockEnd = System.nanoTime()
            val counters = summary.counters()
            wasProductive = counters.nodesCreated() + counters.relationshipsCreated() > 0
            shouldSchedule |= wasProductive

            println(s"Block of ${nameOf(id)} generated ${counters.nodesCreated()} nodes and ${counters.relationshipsCreated()} edges, took ${prettyTime(blockEnd-blockStart)}.")
          } while( wasProductive ) 
          val fragmentEnd = System.nanoTime()

          println(s"Fragment ${nameOf(id)} took ${prettyTime(fragmentEnd-fragmentStart)}.")

          if( shouldSchedule && affects.contains(id) ){
            for(affected <- affects(id) if !isTodo(affected)) {
              println(s"Scheduling affected fragment: ${nameOf(affected)}.")
              todo += affected
              isTodo += affected
            }
          }
        }
      }
    } finally {
      val queryEnd = System.nanoTime()
      println(s"Execution time: ${prettyTime(queryEnd-queryStart)}")
    }
  }
}


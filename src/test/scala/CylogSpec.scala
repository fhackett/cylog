package ca.uwaterloo.cylog.test

import scala.util.Using

import org.scalatest._

class CylogSpec extends FlatSpec with Matchers {

  import org.testcontainers.containers.Neo4jContainer
  import org.testcontainers.utility.MountableFile

  def withNeo4J(test : Neo4jContainer[_]=>Any) : Any = {
    Using.resource(new Neo4jContainer()
      .withoutAuthentication().asInstanceOf[Neo4jContainer[_]]
      .withPlugins(MountableFile.forHostPath("apoc-3.5.0.4-all.jar")).asInstanceOf[Neo4jContainer[_]]
      .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*,algo.*")) 
    { neo4jContainer : Neo4jContainer[_] =>
      neo4jContainer.start()
      test(neo4jContainer)
    }
  }

  import org.neo4j.driver.v1.{AuthTokens,Driver,GraphDatabase,Session,StatementResult,TransactionConfig}

  def withDriver(test : Driver=>Any) : Any =
    withNeo4J { neo4JContainer =>
      Using.resource(GraphDatabase.driver(neo4JContainer.getBoltUrl(), AuthTokens.none())) { driver =>
        test(driver)
      }
    }

  def runDatalog(dl : String, session : Session) : Unit = {
    import ca.uwaterloo.cylog._

    val directives = Parser.tryParse(dl)
    val blocks = Compiler.compile(directives)

    for(block <- blocks) {
      println("---BLOCK---")
      for((head, dependsOn, part) <- block) {
        println(s"HEAD: $head, dependsOn: $dependsOn")
        println(part)
        println("--")
      }
    }
    
    Runner.run(blocks, session)
  } 

  it should "implicitly deduplicate relations" in withDriver { driver =>
    val session = driver.session()
    
    // create 2 parallel relations
    locally {
      val result = session.run("""
        CREATE (a)-[:foo]->(b)
        CREATE (a)-[:foo]->(b)
        """, TransactionConfig.empty())
      val summary = result.consume()

      val counters = summary.counters()
      assert(counters.nodesCreated() == 2)
      assert(counters.relationshipsCreated() == 2)
    }

    runDatalog("""
      .edge foo

      .decl foo2(a : vertex, b : vertex)
      foo2(a, b) :- foo(_, a, b).
      .output foo2
      """, session)

    locally {
      val result = session.run("""
        MATCH (f:foo2)-[:`foo2$a`]->(a), (f)-[:`foo2$b`]->(b)
        RETURN count(*) AS count
        """, TransactionConfig.empty())
      assert(result.single().get("count").asInt() == 1)
    }
  }

  it should "calculate the same number of distinct paths as -[:rel*]->" in withDriver { driver =>
    val session = driver.session()
    
    locally {
      val result = session.run("""
        CALL apoc.generate.ba(10, 5, 'label', 'rel')
        """, TransactionConfig.empty())
      result.consume()
    }

    val numPaths = locally {
      val result = session.run("""
        MATCH (a)-[:rel*]->(b)
        WITH DISTINCT *
        RETURN count(*) AS count
        """, TransactionConfig.empty())
      result.single().get("count").asInt()
    }

    println(s"numPaths=$numPaths")

    runDatalog("""
      .edge rel

      .decl trans(from : vertex, to : vertex)
      trans(from, to) :- rel(_, from, to).
      trans(from, to) :- trans(from, mid), trans(mid, to).
      .output trans
      """, session)

    locally {
      // number of instances of trans should equal the number of distinct paths (2 ways to get from a to b counts as 1)
      val result = session.run("""
        MATCH (t:trans)-[:`trans$from`]->(from), (t)-[:`trans$to`]->(to)
        RETURN count(*) as count
        """, TransactionConfig.empty())
      assert(result.single().get("count").asInt() == numPaths)
    }
  }
}


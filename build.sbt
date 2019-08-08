scalaVersion := "2.13.0"

name := "gremlog"
organization := "ca.uwaterloo"
version := "0.1"

// parser
libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.1.3"
// gremlin stuff
//libraryDependencies += "org.apache.tinkerpop" % "gremlin-core" % "3.4.2"
//libraryDependencies += "org.apache.tinkerpop" % "tinkergraph-gremlin" % "3.4.2"

// neo4j stuff
//libraryDependencies += "org.apache.tinkerpop" % "neo4j-gremlin" % "3.4.2"
// libraryDependencies += "org.neo4j" % "neo4j-tinkerpop-api-impl" % "0.9-3.4.0"
//libraryDependencies += "org.neo4j" % "neo4j-tinkerpop-api-impl" % "0.7-3.2.3"

// neo4j driver
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.7.2"


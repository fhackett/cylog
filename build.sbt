scalaVersion := "2.13.0"

name := "gremlog"
organization := "ca.uwaterloo"
version := "0.1"

// parser
libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.1.3"
// gremlin stuff
libraryDependencies += "org.apache.tinkerpop" % "gremlin-core" % "3.4.1"
libraryDependencies += "org.apache.tinkerpop" % "tinkergraph-gremlin" % "3.4.1"


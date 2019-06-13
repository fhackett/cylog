scalaVersion := "2.12.8"

name := "gremlog"
organization := "ca.uwaterloo"
version := "0.1"

// parser
libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.1.3"
// gremlin stuff
libraryDependencies += "org.apache.tinkerpop" % "gremlin-core" % "3.4.1"
libraryDependencies += "org.apache.tinkerpop" % "tinkergraph-gremlin" % "3.4.1"

// logs, files and config
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.8.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"


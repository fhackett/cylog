scalaVersion := "2.13.0"

name := "cylog"
organization := "ca.uwaterloo"
version := "0.1"

// parser
libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.1.3"

// neo4j driver
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.7.2"

// testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
// run neo4j in a docker container, because embedded is broken on Scala 2.13 :(
libraryDependencies += "org.testcontainers" % "neo4j" % "1.12.1" % Test
libraryDependencies += "org.testcontainers" % "testcontainers" % "1.12.1" % Test

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.28" % Test


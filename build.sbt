name := "pancake-db-client"

version := "0.2.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "com.github.sbt" %% "sbt-jni-core" % "1.5.3",
  "com.pancakedb" % "pancake-db-idl" % "0.2.0",

  "org.scalatest" %% "scalatest" % "3.2.11" % Test
)

name := "pancake-db-client"

version := "0.1.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java-util" % "3.18.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "com.github.sbt" %% "sbt-jni-core" % "1.5.3",
  "com.pancakedb" % "pancake-db-idl" % "0.1.0",

  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

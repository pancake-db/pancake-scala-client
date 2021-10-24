name := "pancake-db-scala-client"

version := "0.0.0-alpha.0_osx"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java-util" % "3.18.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "com.pancakedb" % "idl" % "0.0.0-alpha.0",

  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

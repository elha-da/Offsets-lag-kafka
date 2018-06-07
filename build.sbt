name := "KafkaConsumerOffsetsUsage"

version := "1.0"
scalaVersion := "2.12.5"

val circeV = "0.9.0"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.11.0.0", // "0.10.2.0", // "1.0.0",
  "com.typesafe.akka" %% "akka-actor" % "2.5.11",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.5.11",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
  "io.circe" %% "circe-core" % circeV,
  "io.circe" %% "circe-generic" % circeV,
  "io.circe" %% "circe-parser" % circeV,
  "io.circe" %% "circe-optics" % circeV
//  "org.apache.spark" %% s"spark-core_2.11" % sparkVersion cross CrossVersion.Disabled

).map(_ exclude("org.slf4j", "slf4j-log4j12") exclude("junit", "junit")) // exclude("log4j"))

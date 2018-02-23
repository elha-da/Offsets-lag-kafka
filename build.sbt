name := "KafkaConsumerOffsetsUsage"

version := "1.0"

scalaVersion := "2.12.4"
val circeV = "0.9.0"

libraryDependencies ++= Seq(
  "org.apache.kafka"  %% "kafka"                % "0.11.0.1",//"1.0.0",//
  "com.typesafe.akka" %% "akka-actor"           % "2.5.9",
  "com.typesafe.akka" %% "akka-stream-kafka"    % "0.18",
  "io.circe" %% "circe-core" % circeV,
  "io.circe" %% "circe-generic" % circeV,
  "io.circe" %% "circe-parser" % circeV,
  "io.circe" %% "circe-optics" % circeV
).map(_ exclude("org.slf4j", "slf4j-log4j12") exclude("junit", "junit"))// exclude("log4j", "log4j"))






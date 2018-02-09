name := "KafkaConsumerOffsetsUsage"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.apache.kafka"  %% "kafka"                % "1.0.0",
  "com.typesafe.akka" %% "akka-actor"           % "2.5.9",
  "com.typesafe.akka" %% "akka-stream-kafka"    % "0.18"
).map(_ exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j") exclude("junit", "junit"))






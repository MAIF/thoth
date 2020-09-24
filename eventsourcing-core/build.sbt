import Dependencies._

organization := "fr.maif"

name := "eventsourcing-core"

scalaVersion := "2.12.12"

resolvers ++=  Seq(
  Resolver.jcenterRepo,
  Resolver.bintrayRepo("maif-functional-java", "maven")
)

libraryDependencies ++= Seq(
  "io.vavr"                         % "vavr"                       % vavrVersion,
  "io.vavr"                         % "vavr-jackson"               % vavrVersion,
  "com.typesafe.akka"               %% "akka-stream"               % akkaVersion,
  "com.typesafe.akka"               %% "akka-stream-kafka"         % alpakkaKafkaVersion,
  "com.fasterxml.uuid"              % "java-uuid-generator"        % "3.1.5",
  "com.fasterxml.jackson.datatype"  % "jackson-datatype-jdk8"      % jacksonVersion,
  "com.fasterxml.jackson.datatype"  % "jackson-datatype-jsr310"    % jacksonVersion,
  "fr.maif"                         % "functional-json"            % functionalJsonVersion,
  "com.typesafe.akka"               %% "akka-testkit"              % akkaVersion % Test,
  "com.typesafe.akka"               %% "akka-stream-testkit"       % akkaVersion % Test,
  "com.typesafe.akka"               %% "akka-stream-kafka-testkit" % alpakkaKafkaVersion % Test,
  "org.assertj"                     % "assertj-core"               % "3.10.0" % Test,
  "com.h2database"                  % "h2"                         % "1.4.197" % Test,
  "org.mockito"                     % "mockito-core"               % "2.22.0"  % Test,
  "org.junit.platform"              % "junit-platform-launcher"    % "1.4.2" % Test,
  "org.junit.platform"              % "junit-platform-commons"     % "1.4.2" % Test,
  "org.junit.jupiter"               % "junit-jupiter-engine"       % "5.4.2" % Test,
  "org.junit.vintage"               % "junit-vintage-engine"       % "5.4.2" % Test,
  "net.aichler"                     % "jupiter-interface"          % "0.8.2" % Test,
  "org.scalatest"                   %% "scalatest"                 % "3.0.8" % Test,
  "org.testcontainers"              % "kafka"                      % "1.14.3" % Test
)

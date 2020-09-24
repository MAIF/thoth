import sbt.Keys.organization
import Dependencies._

organization := "fr.maif"

name := "eventsourcing-jooq"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "io.vavr"                         % "vavr"                      % vavrVersion,
  "com.typesafe.akka"               %% "akka-stream"              % akkaVersion,
  "com.typesafe.akka"               %% "akka-stream-kafka"        % alpakkaKafkaVersion,
  "org.postgresql"                  % "postgresql"                % "42.2.5",
  "org.jooq"                        % "jooq"                      % jooqVersion,
  "javax.annotation"                % "jsr250-api"                % "1.0",
  "com.typesafe.akka"               %% "akka-testkit"             % akkaVersion % Test,
  "com.typesafe.akka"               %% "akka-stream-testkit"      % akkaVersion % Test,
  "org.assertj"                     % "assertj-core"              % "3.10.0" % Test,
  "com.h2database"                  % "h2" % "1.4.197"            % Test,
  "org.junit.platform"              % "junit-platform-launcher"   % "1.4.2" % Test,
  "org.junit.platform"              % "junit-platform-commons"    % "1.4.2" % Test,
  "org.junit.jupiter"               % "junit-jupiter-engine"      % "5.4.2" % Test,
  "org.junit.vintage"               % "junit-vintage-engine"      % "5.4.2" % Test,
  "net.aichler"                     % "jupiter-interface"         % "0.8.2" % Test,
  "org.mockito"                     % "mockito-core"              % "2.22.0" % Test
)



import Dependencies._

organization := "fr.maif"

name := "thoth-kafka-goodies"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"               % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka"         % alpakkaKafkaVersion,
  "com.typesafe.akka" %% "akka-testkit"              % akkaVersion         % Test,
  "com.typesafe.akka" %% "akka-stream-testkit"       % akkaVersion         % Test,
  "com.typesafe.akka" %% "akka-stream-kafka-testkit" % alpakkaKafkaVersion % Test,
  "org.assertj"        % "assertj-core"              % "3.10.0"            % Test,
  "org.junit.platform" % "junit-platform-launcher"   % "1.4.2"             % Test,
  "org.junit.platform" % "junit-platform-commons"    % "1.4.2"             % Test,
  "org.junit.jupiter"  % "junit-jupiter-engine"      % "5.4.2"             % Test,
  "org.junit.vintage"  % "junit-vintage-engine"      % "5.4.2"             % Test,
  "net.aichler"        % "jupiter-interface"         % "0.9.1"             % Test,
  "org.scalatest"     %% "scalatest"                 % "3.0.8"             % Test,
  "org.testcontainers" % "kafka"                     % "1.15.1"            % Test
)

javacOptions in Compile ++= Seq("-source", "8", "-target", "8", "-Xlint:unchecked", "-Xlint:deprecation")

// Skip the javadoc for the moment
sources in (Compile, doc) := Seq.empty

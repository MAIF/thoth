import sbt.Keys.organization
import Dependencies._

organization := "fr.maif"

name := "thoth-jooq"

libraryDependencies ++= Seq(
  "io.vavr"            % "vavr"                    % vavrVersion,
  "com.typesafe.akka" %% "akka-stream"             % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka"       % alpakkaKafkaVersion,
  "org.postgresql"     % "postgresql"              % "42.2.5",
  "org.jooq"           % "jooq"                    % jooqVersion,
  "javax.annotation"   % "jsr250-api"              % "1.0",
  "com.typesafe.akka" %% "akka-testkit"            % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit"     % akkaVersion % Test,
  "com.zaxxer"         % "HikariCP"                % "4.0.3"     % Test,
  "org.assertj"        % "assertj-core"            % "3.10.0"    % Test,
  "com.h2database"     % "h2"                      % "1.4.197"   % Test,
  "org.junit.platform" % "junit-platform-launcher" % "1.4.2"     % Test,
  "org.junit.platform" % "junit-platform-commons"  % "1.4.2"     % Test,
  "org.junit.jupiter"  % "junit-jupiter-engine"    % "5.4.2"     % Test,
  "org.junit.vintage"  % "junit-vintage-engine"    % "5.4.2"     % Test,
  "net.aichler"        % "jupiter-interface"       % "0.9.1"     % Test,
  "org.mockito"        % "mockito-core"            % "2.22.0"    % Test,
  "org.testng"         % "testng"                  % "6.3"       % Test,
  "org.testcontainers" % "postgresql"              % "1.15.0"    % Test,
  "org.testcontainers" % "kafka"                   % "1.15.0"    % Test,
  "org.slf4j"          % "slf4j-api"               % "1.7.30"    % Test,
  "org.slf4j"          % "slf4j-simple"            % "1.7.30"    % Test
)

testNGSuites := Seq(((resourceDirectory in Test).value / "testng.xml").absolutePath)

javacOptions in Compile ++= Seq("-source", "8", "-target", "8", "-Xlint:unchecked", "-Xlint:deprecation")

// Skip the javadoc for the moment
sources in (Compile, doc) := Seq.empty

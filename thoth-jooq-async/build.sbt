import sbt.Keys.organization
import Dependencies._

organization := "fr.maif"

name := "thoth-jooq-async"

libraryDependencies ++= Seq(
  "io.vavr"            % "vavr"                    % vavrVersion,
  "org.jooq"           % "jooq"                    % jooqVersion,
  "fr.maif"           %% "jooq-async-api"          % jooqAsyncVersion,
  "fr.maif"           %% "jooq-async-jdbc"         % jooqAsyncVersion % Test,
  "fr.maif"           %% "jooq-async-reactive"     % jooqAsyncVersion % Test,
  "org.assertj"        % "assertj-core"            % "3.10.0"         % Test,
  "org.postgresql"     % "postgresql"              % "42.2.5"         % Test,
  "org.junit.platform" % "junit-platform-launcher" % "1.4.2"          % Test,
  "org.junit.platform" % "junit-platform-commons"  % "1.4.2"          % Test,
  "org.junit.jupiter"  % "junit-jupiter-engine"    % "5.4.2"          % Test,
  "org.junit.vintage"  % "junit-vintage-engine"    % "5.4.2"          % Test,
  "net.aichler"        % "jupiter-interface"       % "0.9.1"          % Test
)

Compile / javacOptions ++= Seq(
  "-source",
  "8",
  "-target",
  "8",
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

// Skip the javadoc for the moment
Compile / doc / sources := Seq.empty

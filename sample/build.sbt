import Dependencies._

organization := "fr.maif"

name := "sample"

scalaVersion := "2.12.12"

Compile / compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
"org.springframework.boot"  % "spring-boot-starter"      % "2.4.3",
"org.springframework.boot"  % "spring-boot-starter-web"  % "2.4.3",
"org.testcontainers"        % "postgresql"               % "1.16.3" % Test,
"org.testcontainers"        % "kafka"                    % "1.16.3" % Test,
"org.testcontainers"        % "junit-jupiter"            % "1.16.3" % Test,
"org.springframework.boot"  % "spring-boot-starter-test" % "2.4.3"  % Test,
"org.assertj"               % "assertj-core"             % "3.19.0" % Test,
"net.aichler"               % "jupiter-interface"        % "0.9.1"  % Test,
"org.junit.platform"        % "junit-platform-launcher"  % "1.4.2"  % Test,
"org.junit.platform"        % "junit-platform-commons"   % "1.4.2"  % Test,
"org.junit.jupiter"         % "junit-jupiter-engine"     % "5.4.2"  % Test,
"org.junit.vintage"         % "junit-vintage-engine"     % "5.4.2"  % Test
)

Compile / javacOptions ++= Seq(
  "-source",
  "16",
  "-target",
  "16",
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

import Dependencies._

organization := "fr.maif"

name := "sample"

scalaVersion := "2.12.12"


libraryDependencies += "org.springframework.boot" % "spring-boot-starter" % "2.4.3"
libraryDependencies += "org.springframework.boot" % "spring-boot-starter-web" % "2.4.3"
libraryDependencies += "org.springframework.kafka" % "spring-kafka" % "2.6.6"
libraryDependencies += "org.testcontainers" % "postgresql" % "1.15.2" % Test
libraryDependencies += "org.testcontainers" % "kafka" % "1.15.2" % Test
libraryDependencies += "org.testcontainers" % "junit-jupiter" % "1.15.2" % Test
libraryDependencies += "org.springframework.boot" % "spring-boot-starter-test" % "2.4.3" % Test
libraryDependencies += "org.assertj" % "assertj-core" % "3.19.0" % Test
libraryDependencies += "net.aichler" % "jupiter-interface" % "0.9.1"     % Test
libraryDependencies += "org.junit.platform" % "junit-platform-launcher" % "1.4.2"     % Test
libraryDependencies += "org.junit.platform" % "junit-platform-commons"  % "1.4.2"     % Test
libraryDependencies += "org.junit.jupiter"  % "junit-jupiter-engine"    % "5.4.2"     % Test
libraryDependencies += "org.junit.vintage"  % "junit-vintage-engine"    % "5.4.2"     % Test

javacOptions in Compile ++= Seq("-source", "15", "-target", "15", "-Xlint:unchecked", "-Xlint:deprecation",  "--enable-preview")

import Dependencies._

organization := "fr.maif"

name := "demo-postgres-kafka-reactive"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "fr.maif"                         % "functional-json"            % functionalJsonVersion
)

javacOptions in Compile ++= Seq("-source", "16", "-target", "16", "-Xlint:unchecked", "-Xlint:deprecation")

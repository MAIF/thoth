import Dependencies._

organization := "fr.maif"

name := "demo-in-memory"

scalaVersion := "2.12.12"

javacOptions in Compile ++= Seq("-source", "15", "-target", "15", "-Xlint:unchecked", "-Xlint:deprecation")
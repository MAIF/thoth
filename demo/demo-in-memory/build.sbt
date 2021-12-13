import Dependencies._

organization := "fr.maif"

name := "demo-in-memory"

scalaVersion := "2.12.12"

Compile / javacOptions ++= Seq("-source", "16", "-target", "16", "-Xlint:unchecked", "-Xlint:deprecation")
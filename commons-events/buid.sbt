import Dependencies._

organization := "fr.maif"

name := "commons-events"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "io.vavr"                         % "vavr"                      % vavrVersion
)

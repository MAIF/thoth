import Dependencies._

organization := "fr.maif"

name := "commons-events"

libraryDependencies ++= Seq(
  "io.vavr" % "vavr" % vavrVersion
)

javacOptions in Compile ++= Seq("-source", "11", "-target", "11", "-Xlint:unchecked", "-Xlint:deprecation")

// Skip the javadoc for the moment
sources in (Compile, doc) := Seq.empty

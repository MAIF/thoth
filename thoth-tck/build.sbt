import Dependencies._

organization := "fr.maif"

name := "thoth-tck"

libraryDependencies ++= Seq(
  "org.assertj"        % "assertj-core" % "3.10.0",
  "org.testng"         % "testng"       % "6.3",
  "com.typesafe.akka" %% "akka-stream"  % akkaVersion % Test,
  "org.mockito"        % "mockito-core" % "3.6.28"    % Test
)

testNGSuites := Seq(((Test / resourceDirectory).value / "testng.xml").absolutePath)

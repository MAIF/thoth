import ReleaseTransformations._

name := "scribe"
organization := "fr.maif"

scalaVersion := "2.12.12"

val res = Seq(
  Resolver.jcenterRepo,
  Resolver.bintrayRepo("maif-functional-java", "maven")
)

resolvers ++= res

lazy val root = (project in file("."))
  .aggregate(
    `commons-events`,
    `scribe-core`,
    `scribe-jooq`,
    `scribe-jooq-async`,
    `demo-postgres-kafka`,
    `demo-in-memory`
  )
  .enablePlugins(NoPublish, GitVersioning, GitBranchPrompt)
  .disablePlugins(BintrayPlugin)

lazy val `commons-events` = project
  .settings(publishCommonsSettings: _*)

lazy val `scribe-jooq-async` = project
  .dependsOn(`scribe-core`)
  .settings(publishCommonsSettings: _*)

lazy val `scribe-core` = project
  .dependsOn(`commons-events`)
  .settings(publishCommonsSettings: _*)

lazy val `scribe-jooq` = project
  .dependsOn(`scribe-core`)
  .settings(publishCommonsSettings: _*)

lazy val `demo-postgres-kafka` = project
  .dependsOn(`scribe-jooq`)

lazy val `demo-in-memory` = project
  .dependsOn(`scribe-core`)

javacOptions in Compile ++= Seq("-source", "15", "-target", "8", "-Xlint:unchecked", "-Xlint:deprecation")

testFrameworks := Seq(TestFrameworks.JUnit)
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

(parallelExecution in Test) := false

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

lazy val githubRepo = "maif/scribe"

lazy val publishCommonsSettings = Seq(
  homepage := Some(url(s"https://github.com/$githubRepo")),
  startYear := Some(2018),
  bintrayOmitLicense := true,
  crossPaths := false,
  scmInfo := Some(
    ScmInfo(
      url(s"https://github.com/$githubRepo"),
      s"scm:git:https://github.com/$githubRepo.git",
      Some(s"scm:git:git@github.com:$githubRepo.git")
    )
  ),
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
  developers := List(
    Developer("alexandre.delegue", "Alexandre Delègue", "", url(s"https://github.com/larousso")),
    Developer("benjamin.cavy", "Benjamin Cavy", "", url(s"https://github.com/ptitFicus")),
    Developer("gregory.bevan", "Grégory Bévan", "", url(s"https://github.com/GregoryBevan")),
    Developer("georges.ginon", "Georges Ginon", "", url(s"https://github.com/ftoumHub"))
  ),
  releaseCrossBuild := true,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  bintrayVcsUrl := Some(s"scm:git:git@github.com:$githubRepo.git"),
  resolvers ++= res,
  bintrayOrganization := Some("maif-functional-java"),
  bintrayRepository := "maven",
  pomIncludeRepository := { _ =>
    false
  }
)
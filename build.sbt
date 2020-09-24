import ReleaseTransformations._

name := "eventsourcing"
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
    `eventsourcing-core`,
    `eventsourcing-jooq`,
    `eventsourcing-jooq-async`
  )
  .enablePlugins(NoPublish, GitVersioning, GitBranchPrompt)
  .disablePlugins(BintrayPlugin)

lazy val `commons-events` = project
  .settings(publishCommonsSettings: _*)

lazy val `eventsourcing-jooq-async` = project
  .dependsOn(`eventsourcing-core`)
  .settings(publishCommonsSettings: _*)

lazy val `eventsourcing-core` = project
  .dependsOn(`commons-events`)
  .settings(publishCommonsSettings: _*)

lazy val `eventsourcing-jooq` = project
  .dependsOn(`eventsourcing-core`)
  .settings(publishCommonsSettings: _*)

javacOptions in Compile ++= Seq("-source", "8", "-target", "8", "-Xlint:unchecked", "-Xlint:deprecation")

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

lazy val githubRepo = "maif/java-eventsourcing"

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
  developers := List(
    Developer("alexandre.delegue", "Alexandre Delègue", "", url(s"https://github.com/larousso")),
    Developer("benjamin.cavy", "Benjamin Cavy", "", url(s"https://github.com/ptitFicus"))
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
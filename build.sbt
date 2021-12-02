import ReleaseTransformations._

name := "thoth"
organization := "fr.maif"

resolvers ++= Seq(Resolver.jcenterRepo)

val mainScalaVersion = "2.12.13"
val scalaVersions    = List("2.13.5", mainScalaVersion)

scalaVersion := mainScalaVersion
crossScalaVersions := scalaVersions

usePgpKeyHex("5B6BE1966878E3AE16B85BC975B8BA741462DEA9")
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
sonatypeCredentialHost := "s01.oss.sonatype.org"

lazy val root = (project in file("."))
  .aggregate(
    `commons-events`,
    `thoth-kafka-goodies`,
    `thoth-core`,
    `thoth-jooq`,
    `thoth-jooq-async`,
    `demo-postgres-kafka`,
    `demo-in-memory`,
    `sample`,
    `thoth-documentation`,
    `thoth-tck`
  )
  .enablePlugins(GitVersioning, GitBranchPrompt)
  .settings(
    publish / skip := true
  )

lazy val `demo-postgres-kafka` = (project in file("./demo/demo-postgres-kafka"))
  .dependsOn(`thoth-jooq`)
  .settings(
    publish / skip := true
  )

lazy val `sample` = project
  .dependsOn(`thoth-jooq`)
  .settings(
    publish / skip := true
  )

lazy val `demo-in-memory` = (project in file("./demo/demo-in-memory"))
  .dependsOn(`thoth-core`)
  .settings(
    publish / skip := true
  )

lazy val `thoth-documentation` = project
  .settings(
    publish / skip := true
  )

lazy val `thoth-tck` = project
  .dependsOn(`thoth-core`)
  .enablePlugins(TestNGPlugin)
  .settings(
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    scalaVersion := mainScalaVersion,
    crossScalaVersions := scalaVersions,
    crossPaths := true
  )

lazy val `demo-postgres-kafka-reactive` =
  (project in file("./demo/demo-postgres-kafka-reactive"))
    .dependsOn(`thoth-core`, `thoth-jooq-async`)
    .settings(
      publish / skip := true
    )

lazy val `commons-events` = project
  .settings(
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    scalaVersion := mainScalaVersion,
    crossPaths := false
  )

lazy val `thoth-kafka-goodies` = project
  .settings(
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    scalaVersion := mainScalaVersion,
    crossScalaVersions := scalaVersions,
    crossPaths := true
  )

lazy val `thoth-jooq-async` = project
  .dependsOn(`thoth-core`)
  .settings(
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    scalaVersion := mainScalaVersion,
    crossScalaVersions := scalaVersions,
    crossPaths := true
  )

lazy val `thoth-core` = project
  .dependsOn(`commons-events`, `thoth-kafka-goodies`)
  .settings(
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    scalaVersion := mainScalaVersion,
    crossScalaVersions := scalaVersions,
    crossPaths := true
  )

lazy val `thoth-jooq` = project
  .dependsOn(`thoth-core`, `thoth-tck` % "compile->test")
  .enablePlugins(TestNGPlugin)
  .settings(
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    scalaVersion := mainScalaVersion,
    crossScalaVersions := scalaVersions,
    crossPaths := true
  )

Compile / javacOptions ++= Seq(
  "-source",
  "16",
  "-target",
  "8",
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

testFrameworks := Seq(TestFrameworks.JUnit)
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

(Test / parallelExecution) := false

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

lazy val githubRepo   = "maif/thoth"

inThisBuild(
  List(
    homepage := Some(url(s"https://github.com/$githubRepo")),
    startYear := Some(2018),
    scmInfo := Some(
        ScmInfo(
          url(s"https://github.com/$githubRepo"),
          s"scm:git:https://github.com/$githubRepo.git",
          Some(s"scm:git:git@github.com:$githubRepo.git")
        )
      ),
    licenses := Seq(
        ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
      ),
    developers := List(
        Developer(
          "alexandre.delegue",
          "Alexandre Delègue",
          "",
          url(s"https://github.com/larousso")
        ),
        Developer(
          "benjamin.cavy",
          "Benjamin Cavy",
          "",
          url(s"https://github.com/ptitFicus")
        ),
        Developer(
          "gregory.bevan",
          "Grégory Bévan",
          "",
          url(s"https://github.com/GregoryBevan")
        ),
        Developer(
          "georges.ginon",
          "Georges Ginon",
          "",
          url(s"https://github.com/ftoumHub")
        )
      ),
    releaseCrossBuild := true,
    Test / publishArtifact := false
  )
)

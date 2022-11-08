import sbt.project

lazy val `thoth-documentation` = (project in file("."))
  .enablePlugins(ParadoxPlugin)
  .settings(
    publish / skip := true,
    name := "Thoth doc",
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxGroups := Map("Language" -> Seq("Java")),
    Compile / paradoxProperties ++= Map(
        "version"               -> version.value,
        "scalaVersion"          -> scalaVersion.value,
        "scalaBinaryVersion"    -> scalaBinaryVersion.value,
        "download_zip.base_url" -> s"https://github.com/maif/thoth/releases/download/v${version}/thoth.zip",
        "download_jar.base_url" -> s"https://github.com/maif/thoth/releases/download/v${version}/thoth.jar"
      ),
    watchSources ++= Seq(sourceDirectory.value / "main" / "paradox")
  )

lazy val generateDoc           = taskKey[Unit]("Copy doc")

generateDoc := {
  val _           = (Compile / paradox).value
  val paradoxFile = target.value / "paradox" / "site" / "main"
  val targetDocs  = `thoth-documentation`.base.getParentFile / ".." / "docs" / "manual"
  IO.delete(targetDocs)
  IO.copyDirectory(paradoxFile, targetDocs)
}

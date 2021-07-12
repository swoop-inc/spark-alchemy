ThisBuild / organization := "com.swoop"
ThisBuild / version := scala.io.Source.fromFile("VERSION").mkString.stripLineEnd

ThisBuild / scalaVersion := "2.12.11"
ThisBuild / crossScalaVersions := Seq("2.12.11")

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

val sparkVersion = "3.1.2"

lazy val scalaSettings = Seq(
  scalaVersion := "2.12.11",
  crossScalaVersions := Seq("2.12.11"),
  scalacOptions in(Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value + "/docs/root-doc.txt"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits"),
  javacOptions in(Compile, doc) ++= Seq("-notimestamp", "-linksource"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val alchemy = (project in file("."))
  .settings(
    name := "spark-alchemy",
    scalaSource in Compile := baseDirectory.value / "alchemy/src/main/scala",
    scalaSource in Test := baseDirectory.value / "alchemy/src/test/scala",
    resourceDirectory in Compile := baseDirectory.value / "alchemy/src/main/resources",
    resourceDirectory in Test := baseDirectory.value / "alchemy/src/test/resources",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.2" % Test withSources(),
      "net.agkn" % "hll" % "1.6.0" withSources(),
      "org.postgresql" % "postgresql" % "42.2.8" % Test withSources(),
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
    ),
    fork in Test := true, // required for Spark
    scalaSettings
  )
  .enablePlugins(SiteScaladocPlugin)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(GitVersioning, GitBranchPrompt)

lazy val docs = project
  .in(file("docs"))
  .settings(
    moduleName := "spark-alchemy-docs",
    name := moduleName.value,
    scalaSettings,
    micrositeSettings
  )
  .dependsOn(alchemy)
  .enablePlugins(MicrositesPlugin)

lazy val micrositeSettings = Seq(
  micrositeName := "Spark Alchemy",
  micrositeDescription := "Useful extensions to Apache Spark",
  micrositeAuthor := "Swoop",
  micrositeHomepage := "https://www.swoop.com",
  micrositeBaseUrl := "/spark-alchemy",
  micrositeDocumentationUrl := "/spark-alchemy/docs.html",
  micrositeGithubOwner := "swoop-inc",
  micrositeGithubRepo := "spark-alchemy",
  micrositeHighlightTheme := "tomorrow",
  micrositeTheme := "pattern",
  micrositePushSiteWith := GitHub4s,
  micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
  micrositeImgDirectory := (Compile / resourceDirectory).value / "site" / "images",
  micrositeCssDirectory := (Compile / resourceDirectory).value / "site" / "styles",
  micrositeJsDirectory := (Compile / resourceDirectory).value / "site" / "scripts"
)


// Speed up dependency resolution (experimental)
// @see https://www.scala-sbt.org/1.0/docs/Cached-Resolution.html
ThisBuild / updateOptions := updateOptions.value.withCachedResolution(true)

// @see https://wiki.scala-lang.org/display/SW/Configuring+SBT+to+Generate+a+Scaladoc+Root+Page
autoAPIMappings := true

buildInfoPackage := "com.swoop.alchemy"

// SBT header settings
ThisBuild / organizationName := "Swoop, Inc"
ThisBuild / startYear := Some(2018)
ThisBuild / licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
ThisBuild / homepage := Some(url("https://swoop-inc.github.io/spark-alchemy/"))
ThisBuild / developers ++= List(
  Developer("ssimeonov", "Simeon Simeonov", "@ssimeonov", url("https://github.com/ssimeonov"))
)
ThisBuild / scmInfo := Some(ScmInfo(url("https://github.com/swoop-inc/spark-alchemy"), "git@github.com:swoop-inc/spark-alchemy.git"))
ThisBuild / updateOptions := updateOptions.value.withLatestSnapshots(false)
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := sonatypePublishToBundle.value
Global / useGpgPinentry := true

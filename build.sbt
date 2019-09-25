ThisBuild / organization := "com.swoop"
ThisBuild / version := scala.io.Source.fromFile("VERSION").mkString.stripLineEnd

ThisBuild / scalaVersion := "2.11.8"
ThisBuild / crossScalaVersions := Seq("2.11.8")

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"

val sparkVersion = "2.3.1"

// https://bintray.com/swoop-inc/maven
resolvers += Resolver.bintrayRepo("swoop-inc", "maven")

lazy val alchemy = (project in file("."))
  .aggregate(test)
  .settings(
    name := "spark-alchemy",
    scalaSource in Compile := baseDirectory.value / "alchemy/src/main/scala",
    scalaSource in Test := baseDirectory.value / "alchemy/src/test/scala",
    resourceDirectory in Compile := baseDirectory.value / "alchemy/src/main/resources",
    resourceDirectory in Test := baseDirectory.value / "alchemy/src/test/resources",
    libraryDependencies ++= Seq(
      scalaTest % Test withSources(),
      "com.swoop" %% "spark-test-sugar" % "1.5.0" % Test withSources()
    ),
    libraryDependencies ++= sparkDependencies,
    fork in Test := true // required for Spark
  )

lazy val test = (project in file("alchemy-test"))
  .settings(
    name := "spark-alchemy-test",
    libraryDependencies ++= Seq(
      scalaTest % Test withSources()
    ),
    libraryDependencies ++= sparkDependencies
  )

lazy val sparkDependencies = Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.7" % "provided" withSources(),
  "org.apache.logging.log4j" % "log4j-core" % "2.7" % "provided" withSources(),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
    excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" withSources()
)

enablePlugins(BuildInfoPlugin)
enablePlugins(GitVersioning, GitBranchPrompt)
enablePlugins(MicrositesPlugin)
enablePlugins(SiteScaladocPlugin)
enablePlugins(TutPlugin)

// Speed up dependency resolution (experimental)
// @see https://www.scala-sbt.org/1.0/docs/Cached-Resolution.html
ThisBuild / updateOptions := updateOptions.value.withCachedResolution(true)

// @see https://wiki.scala-lang.org/display/SW/Configuring+SBT+to+Generate+a+Scaladoc+Root+Page
scalacOptions in(Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value + "/docs/root-doc.txt")
scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits")
javacOptions in(Compile, doc) ++= Seq("-notimestamp", "-linksource")
autoAPIMappings := true

buildInfoPackage := "com.swoop.alchemy"

tutSourceDirectory := baseDirectory.value / "docs" / "main" / "tut"
micrositeImgDirectory := baseDirectory.value / "docs" / "main" / "resources" / "site" / "images"
micrositeCssDirectory := baseDirectory.value / "docs" / "main" / "resources" / "site" / "styles"
micrositeJsDirectory := baseDirectory.value / "docs" / "main" / "resources" / "site" / "scripts"

micrositeName := "Spark Alchemy"
micrositeDescription := "Useful extensions to Apache Spark"
micrositeAuthor := "Swoop"
micrositeHomepage := "https://www.swoop.com"
micrositeBaseUrl := "spark-alchemy"
micrositeDocumentationUrl := "/spark-alchemy/docs.html"
micrositeGithubOwner := "swoop-inc"
micrositeGithubRepo := "spark-alchemy"
micrositeHighlightTheme := "tomorrow"

micrositePushSiteWith := GitHub4s
micrositeGithubToken := sys.env.get("GITHUB_TOKEN")

// SBT header settings
ThisBuild / organizationName := "Swoop, Inc"
ThisBuild / startYear := Some(2018)
ThisBuild / licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

// Bintray publishing
ThisBuild / bintrayOrganization := Some("swoop-inc")
ThisBuild / bintrayPackageLabels := Seq("apache", "spark", "apache-spark", "scala", "big-data", "spark-alchemy", "dataset", "swoop")

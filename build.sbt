organization := "com.swoop"
version := "1.0.2"
name := "spark-alchemy"

scalaVersion := "2.12.12"
crossScalaVersions := Seq("2.12.12")

val sparkVersion = "3.0.0"

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
homepage := Some(url("https://swoop-inc.github.io/spark-alchemy/"))
developers ++= List(
  Developer("ssimeonov", "Simeon Simeonov", "@ssimeonov", url("https://github.com/ssimeonov"))
)
scmInfo := Some(ScmInfo(url("https://github.com/swoop-inc/spark-alchemy"), "git@github.com:swoop-inc/spark-alchemy.git"))
updateOptions := updateOptions.value.withLatestSnapshots(false)
publishMavenStyle := true
publishTo := sonatypePublishToBundle.value
Global / useGpgPinentry := true

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.2" % Test withSources(),
  "net.agkn" % "hll" % "1.6.0" withSources(),
  "org.postgresql" % "postgresql" % "42.2.8" % Test withSources(),
  "org.apache.logging.log4j" % "log4j-api" % "2.7" % "provided" withSources(),
  "org.apache.logging.log4j" % "log4j-core" % "2.7" % "provided" withSources(),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
    excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" withSources()
)
fork in Test := true // required for Spark

// SBT header settings
organizationName := "Swoop, Inc"
startYear := Some(2018)
licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))


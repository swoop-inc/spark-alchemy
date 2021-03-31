package com.swoop.test_utils

import org.apache.logging.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.test.{SharedSparkSessionBase, TestSparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.TestSuite

import scala.util.Try


trait SparkSessionSpec extends SharedSparkSessionBase {
  this: TestSuite =>

  override protected def createSparkSession: TestSparkSession = {
    val spark = super.createSparkSession
    configureLoggers(sparkLogLevel)
    spark
  }

  def sparkSession = spark

  def sqlc: SQLContext = sparkSession.sqlContext

  def sc: SparkContext = sparkSession.sparkContext

  protected def sparkLogLevel =
    Try(sys.env("SPARK_LOG_LEVEL")).getOrElse("WARN").toUpperCase match {
      case "DEBUG" => Level.DEBUG
      case "INFO" => Level.INFO
      case "WARN" => Level.WARN
      case _ => Level.ERROR
    }

  protected def configureLoggers(): Unit =
    configureLoggers(sparkLogLevel)

  protected def configureLoggers(logLevel: Level): Unit = {
    // Set logging through log4j v1 APIs also as v2 APIs are too tricky to manage
    org.apache.log4j.Logger.getRootLogger.setLevel(logLevel match {
      case Level.DEBUG => org.apache.log4j.Level.DEBUG
      case Level.INFO => org.apache.log4j.Level.INFO
      case Level.WARN => org.apache.log4j.Level.WARN
      case Level.ERROR => org.apache.log4j.Level.ERROR
    })
  }

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.driver.bindAddress", "127.0.0.1")

}

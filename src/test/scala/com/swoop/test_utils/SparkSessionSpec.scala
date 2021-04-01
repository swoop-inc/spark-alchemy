package com.swoop.test_utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.test.{SharedSparkSessionBase, TestSparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.TestSuite

trait SparkSessionSpec extends SharedSparkSessionBase {
  this: TestSuite =>

  override protected def createSparkSession: TestSparkSession = {
    val spark = super.createSparkSession
    spark
  }

  def sparkSession = spark

  def sqlc: SQLContext = sparkSession.sqlContext

  def sc: SparkContext = sparkSession.sparkContext

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.driver.bindAddress", "127.0.0.1")

}

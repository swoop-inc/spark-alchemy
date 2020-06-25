package org.apache.spark

import org.apache.spark.sql.SparkSession


object SparkTestUtilsEncapsulationViolator {

  def builderWithSparkContext(builder: SparkSession.Builder, sc: SparkContext): SparkSession.Builder =
    builder.sparkContext(sc)

  def cleanupAnyExistingSession(): Unit =
    SparkSession.cleanupAnyExistingSession()

  def utils = org.apache.spark.util.Utils

}

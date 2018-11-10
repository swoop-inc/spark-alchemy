package com.swoop.alchemy.spark.expressions

import org.apache.spark.sql.SparkSession

trait FunctionRegistration {
  def registerFunctions(spark: SparkSession): Unit
}

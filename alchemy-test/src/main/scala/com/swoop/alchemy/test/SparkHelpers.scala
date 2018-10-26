package com.swoop.alchemy.test

import org.apache.spark.sql.{Column, DataFrame}

trait SparkHelpers {

  def stripExpressionIds(s: String): String = "#\\d+".r.unanchored.replaceAllIn(s, "")

  def simpleSql(column: Column): String = stripExpressionIds(column.expr.sql)

  def simplePlan(df: DataFrame): String = stripExpressionIds(df.queryExecution.logical.toString).stripLineEnd

}

object SparkHelpers extends SparkHelpers

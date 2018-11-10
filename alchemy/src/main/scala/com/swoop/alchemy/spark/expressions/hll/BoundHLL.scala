package com.swoop.alchemy.spark.expressions.hll

import org.apache.spark.sql
import org.apache.spark.sql.Column


/** Convenience trait to use HyperLogLog functions with the same error consistently.
  * Spark's own [[sql.functions.approx_count_distinct()]] as well as the granular HLL
  * [[HLLFunctions.hll_init()]] and [[HLLFunctions.hll_init_collection()]] will be
  * automatically parameterized by [[BoundHLL.hllError]].
  */
trait BoundHLL extends Serializable {

  def hllError: Double

  def approx_count_distinct(col: Column): Column =
    sql.functions.approx_count_distinct(col, hllError)

  def approx_count_distinct(colName: String): Column =
    sql.functions.approx_count_distinct(colName, hllError)

  def hll_init(col: Column): Column =
    functions.hll_init(col, hllError)

  def hll_init(columnName: String): Column =
    functions.hll_init(columnName, hllError)

  def hll_init_collection(col: Column): Column =
    functions.hll_init_collection(col, hllError)

  def hll_init_collection(columnName: String): Column =
    functions.hll_init_collection(columnName, hllError)

  def hll_init_agg(col: Column): Column =
    functions.hll_init_agg(col, hllError)

  def hll_init_agg(columnName: String): Column =
    functions.hll_init_agg(columnName, hllError)

  def hll_init_collection_agg(col: Column): Column =
    functions.hll_init_collection_agg(col, hllError)

  def hll_init_collection_agg(columnName: String): Column =
    functions.hll_init_collection_agg(columnName, hllError)

}

object BoundHLL {
  def apply(error: Double): BoundHLL = new BoundHLL {
    def hllError: Double = error
  }
}

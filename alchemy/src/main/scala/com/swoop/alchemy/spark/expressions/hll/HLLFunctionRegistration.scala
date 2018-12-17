package com.swoop.alchemy.spark.expressions.hll

import com.swoop.alchemy.spark.expressions.NativeFunctionRegistration
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

object HLLFunctionRegistration extends NativeFunctionRegistration {

  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[HyperLogLogInitSimple]("hll_init"),
    expression[HyperLogLogInitCollection]("hll_init_collection"),
    expression[HyperLogLogInitSimpleAgg]("hll_init_agg"),
    expression[HyperLogLogInitCollectionAgg]("hll_init_collection_agg"),
    expression[HyperLogLogMerge]("hll_merge"),
    expression[HyperLogLogRowMerge]("hll_row_merge"),
    expression[HyperLogLogCardinality]("hll_cardinality"),
    expression[HyperLogLogIntersectionCardinality]("hll_intersect_cardinality")
  )

}

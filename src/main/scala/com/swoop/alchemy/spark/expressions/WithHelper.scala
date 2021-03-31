package com.swoop.alchemy.spark.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

trait WithHelper {
  def withExpr(expr: Expression): Column = new Column(expr)

  def withAggregateFunction(
    func: AggregateFunction,
    isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }
}

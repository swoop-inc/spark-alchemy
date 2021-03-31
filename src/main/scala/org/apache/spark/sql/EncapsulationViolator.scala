package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{GenericRow, NamedExpression}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.json4s.JsonAST.JValue

object EncapsulationViolator {

  def createAnalysisException(message: String): AnalysisException =
    new AnalysisException(message)

  def parseDataType(jv: JValue): DataType =
    DataType.parseDataType(jv)

  object implicits {

    implicit class EncapsulationViolationSparkSessionOps(val underlying: SparkSession) extends AnyVal {
      def evSessionState: SessionState = underlying.sessionState
    }

    implicit class EncapsulationViolationRowOps(val underlying: GenericRow) extends AnyVal {
      def evValues: Array[Any] = underlying.values
    }

    implicit class EncapsulationViolationColumnOps(val underlying: Column) extends AnyVal {
      def evNamed: NamedExpression = underlying.named

      def metadata: Metadata = underlying.expr match {
        case ne: NamedExpression => ne.metadata
        case other => Metadata.empty
      }
    }

    implicit class EncapsulationViolationDataTypeOps(val underlying: DataType) extends AnyVal {
      def isSameType(other: DataType): Boolean = underlying.sameType(other)

      def jValue: JValue = underlying.jsonValue

      def toNullable: DataType = underlying.asNullable
    }

    implicit class EncapsulationViolationStructTypeOps(val underlying: StructType) extends AnyVal {
      def evMerge(that: StructType): StructType = underlying.merge(that)
    }

  }

}

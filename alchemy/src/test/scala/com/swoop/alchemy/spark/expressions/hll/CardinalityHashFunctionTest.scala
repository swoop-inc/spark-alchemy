package com.swoop.alchemy.spark.expressions.hll

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{Matchers, WordSpec}


class CardinalityHashFunctionTest extends WordSpec with Matchers {

  "Cardinality hash functions" should {
    "account for nulls" in {
      val a = UTF8String.fromString("a")

      allDistinct(Seq(
        null,
        Array.empty[Byte],
        Array.apply(1.toByte)
      ), BinaryType)

      allDistinct(Seq(
        null,
        UTF8String.fromString(""),
        a
      ), StringType)

      allDistinct(Seq(
        null,
        ArrayData.toArrayData(Array.empty),
        ArrayData.toArrayData(Array(null)),
        ArrayData.toArrayData(Array(null, null)),
        ArrayData.toArrayData(Array(a, null)),
        ArrayData.toArrayData(Array(null, a))
      ), ArrayType(StringType))


      allDistinct(Seq(
        null,
        ArrayBasedMapData(Map.empty),
        ArrayBasedMapData(Map(null.asInstanceOf[String] -> null))
      ), MapType(StringType, StringType))

      allDistinct(Seq(
        null,
        InternalRow(null),
        InternalRow(a)
      ), new StructType().add("foo", StringType))

      allDistinct(Seq(
        InternalRow(null, a),
        InternalRow(a, null)
      ), new StructType().add("foo", StringType).add("bar", StringType))
    }
  }

  def allDistinct(values: Seq[Any], dataType: DataType): Unit = {
    val hashed = values.map(x => CardinalityXxHash64Function.hash(x, dataType, 0))
    hashed.distinct.length should be(hashed.length)
  }

}

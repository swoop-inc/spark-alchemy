package com.swoop.alchemy.spark.expressions.hll

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{InterpretedHashFunction, XXH64}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Hash function for Spark data values that is suitable for cardinality counting. Unlike Spark's built-in hashing,
 * it differentiates between different data types and accounts for nulls.
 */
abstract class CardinalityHashFunction extends InterpretedHashFunction {

  override def hash(value: Any, dataType: DataType, seed: Long): Long = {

    def hashWithTag(typeTag: Long) =
      super.hash(value, dataType, hashLong(typeTag, seed))

    value match {
      // change null handling to differentiate between things like Array.empty and Array(null)
      case null => hashLong(seed, seed)
      // add type tags to differentiate between values on their own or in complex types
      case _: Array[Byte] => hashWithTag(-3698894927619418744L)
      case _: UTF8String => hashWithTag(-8468821688391060513L)
      case _: ArrayData => hashWithTag(-1666055126678331734L)
      case _: MapData => hashWithTag(5587693012926141532L)
      case _: InternalRow => hashWithTag(-891294170547231607L)
      // pass through everything else (simple types)
      case _ => super.hash(value, dataType, seed)
    }
  }

}


object CardinalityXxHash64Function extends CardinalityHashFunction {

  override protected def hashInt(i: Int, seed: Long): Long = XXH64.hashInt(i, seed)

  override protected def hashLong(l: Long, seed: Long): Long = XXH64.hashLong(l, seed)

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    XXH64.hashUnsafeBytes(base, offset, len, seed)
  }

}

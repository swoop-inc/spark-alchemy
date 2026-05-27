package com.swoop.alchemy.spark.expressions.hll.factory

import com.clearspring.analytics.stream.cardinality.RegisterSet
import com.swoop.alchemy.spark.expressions.hll.implementation.Instance
import net.agkn.hll.util.BitVector


/**
 * Option for the underlying HLL implementation used by all functions
 */

trait Implementation {
  def createHll(p: Int): Instance

  def deserialize(bytes: Array[Byte]): Instance
}

object Implementation {
  val AGKN = "AGKN"
  val STRM = "STRM"
  val AGGREGATE_KNOWLEDGE = "AGGREGATE_KNOWLEDGE"
  val STREAM_LIB = "STREAM_LIB"
  val BAREBONES_HLL = "BAREBONES_HLL"
  val OPTIONS: Seq[String] = Seq(AGKN, STRM, AGGREGATE_KNOWLEDGE, STREAM_LIB, BAREBONES_HLL)

  // TODO @peter debugging tools, remove:
  def registerSetToSeq(r: RegisterSet): Seq[Int] =
    for (i <- 0 until r.count) yield r.get(i)

  def bitVectorToSeq(b: BitVector): Seq[Long] = {
    val i = b.registerIterator()
    new Iterator[Long] {
      def hasNext = i.hasNext

      def next = i.next()
    }.toArray
  }
}

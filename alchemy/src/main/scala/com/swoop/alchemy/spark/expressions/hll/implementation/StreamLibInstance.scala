package com.swoop.alchemy.spark.expressions.hll.implementation

import com.clearspring.analytics.stream

class StreamLibInstance(val hll: stream.cardinality.HyperLogLogPlus) extends Instance {
  override def offer(hashedValue: Long): Instance = {
    hll.offerHashed(hashedValue)
    this
  }

  override def merge(other: Instance): Instance = {
    if (other.isInstanceOf[StreamLibInstance]) {
      hll.addAll(other.asInstanceOf[StreamLibInstance].hll)
      this
    } else
      throw new IllegalArgumentException(s"Type of HLL to merge does not match this HLL (${hll.getClass.getName})")
  }

  override def serialize: Array[Byte] = hll.getBytes

  def cardinality: Long = hll.cardinality()
}

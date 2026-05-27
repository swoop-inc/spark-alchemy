package com.swoop.alchemy.spark.expressions.hll.implementation

import net.agkn.hll.HLL

class AgKnInstance(val hll: HLL) extends Instance {
  override def offer(hashedValue: Long): Instance = {
    hll.addRaw(hashedValue)
    this
  }

  override def merge(other: Instance): Instance = {
    if (other.isInstanceOf[AgKnInstance]) {
      hll.union(other.asInstanceOf[AgKnInstance].hll)
      this
    } else
      throw new IllegalArgumentException(s"Type of HLL to merge does not match this HLL (${hll.getClass.getName})")
  }

  override def serialize: Array[Byte] = hll.toBytes

  def cardinality: Long = hll.cardinality()
}

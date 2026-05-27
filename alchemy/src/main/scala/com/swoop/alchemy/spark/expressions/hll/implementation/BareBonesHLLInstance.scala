package com.swoop.alchemy.spark.expressions.hll.implementation

import io.github.bareboneslib.bareboneshll.HLLPlusPlus

class BareBonesHLLInstance(var hll: HLLPlusPlus) extends Instance {
  override def offer(hashedValue: Long): Instance = {
    hll.add(hashedValue)
    this
  }

  override def merge(other: Instance): Instance = {
    other match {
      case instance: BareBonesHLLInstance =>
        hll.merge(instance.hll)
        this
      case _ => throw new IllegalArgumentException(s"Type of HLL to merge does not match this HLL (${hll.getClass.getName})")
    }
  }

  override def serialize: Array[Byte] = hll.serialize()

  def cardinality: Long = hll.estimate()
}
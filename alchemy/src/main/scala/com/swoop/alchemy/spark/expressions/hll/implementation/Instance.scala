package com.swoop.alchemy.spark.expressions.hll.implementation

/**
 * Wrapper for instances of different HLL implementations
 *
 * @note `offer`` and `merge`` may just mutate and return the same underlying HLL instance
 */
trait Instance {
  def offer(hashedValue: Long): Instance

  def merge(other: Instance): Instance

  def serialize: Array[Byte]

  def cardinality: Long
}
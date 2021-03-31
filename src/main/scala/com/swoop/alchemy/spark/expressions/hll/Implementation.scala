package com.swoop.alchemy.spark.expressions.hll

import com.clearspring.analytics.stream
import com.clearspring.analytics.stream.cardinality.{HyperLogLogPlus, RegisterSet}
import net.agkn.hll.HLL
import net.agkn.hll.util.BitVector

/**
 * Wrapper for instances of different HLL implementations
 *
 * @note `offer`` and `merge`` may just mutate and return the same underlying HLL instance
 */
sealed trait Instance {
  def offer(hashedValue: Long): Instance

  def merge(other: Instance): Instance

  def serialize: Array[Byte]

  def cardinality: Long
}

class AgKnInstance(val hll: net.agkn.hll.HLL) extends Instance {
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
  val OPTIONS = Seq(AGKN, STRM, AGGREGATE_KNOWLEDGE, STREAM_LIB)


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

case object AgKn extends Implementation {
  override def createHll(p: Int) = new AgKnInstance(new HLL(p, 5))

  override def deserialize(bytes: Array[Byte]) = new AgKnInstance(HLL.fromBytes(bytes))
}

case object StreamLib extends Implementation {
  override def createHll(p: Int) = new StreamLibInstance(new HyperLogLogPlus(p, 0))

  override def deserialize(bytes: Array[Byte]) = new StreamLibInstance(HyperLogLogPlus.Builder.build(bytes))
}


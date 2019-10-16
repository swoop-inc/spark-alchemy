package com.swoop.alchemy.spark.expressions

import java.io.{ByteArrayInputStream, DataInputStream}

import com.clearspring.analytics.stream
import com.clearspring.analytics.stream.cardinality.RegisterSet
import com.clearspring.analytics.util.{Bits, Varint}
import net.agkn.hll.HLL
import net.agkn.hll.serialization.{HLLMetadata, SchemaVersionOne}
import net.agkn.hll.util.BitVector

package object hll {
  val IMPLEMENTATION_CONFIG_KEY = "com.swoop.alchemy.hll.implementation"

  def strmToAgkn(from: stream.cardinality.HyperLogLogPlus): net.agkn.hll.HLL = {
    HLL.fromBytes(strmToAgkn(from.getBytes))
  }

  def strmToAgkn(from: Array[Byte]): Array[Byte] = {
    var bais = new ByteArrayInputStream(from)
    var oi = new DataInputStream(bais)
    val version = oi.readInt
    // the new encoding scheme includes a version field
    // that is always negative.
    if (version >= 0) {
      throw new UnsupportedOperationException("conversion is only supported for the new style encoding scheme")
    }

    val p = Varint.readUnsignedVarInt(oi)
    val sp = Varint.readUnsignedVarInt(oi)
    val formatType = Varint.readUnsignedVarInt(oi)
    if (formatType != 0) {
      throw new UnsupportedOperationException("conversion is only supported for non-sparse representation")
    }

    val size = Varint.readUnsignedVarInt(oi)
    val longArrayBytes = new Array[Byte](size)
    oi.readFully(longArrayBytes)
    val registerSet = new RegisterSet(Math.pow(2, p).toInt, Bits.getBits(longArrayBytes))
    val bitVector = new BitVector(RegisterSet.REGISTER_SIZE, registerSet.count)

    for (i <- 0 until registerSet.count) bitVector.setRegister(i, registerSet.get(i))
    val schemaVersion = new SchemaVersionOne
    val serializer =
      schemaVersion.getSerializer(net.agkn.hll.HLLType.FULL, RegisterSet.REGISTER_SIZE, registerSet.count)
    bitVector.getRegisterContents(serializer)
    var outBytes = serializer.getBytes

    val metadata = new HLLMetadata(
      schemaVersion.schemaVersionNumber(),
      net.agkn.hll.HLLType.FULL,
      p,
      RegisterSet.REGISTER_SIZE,
      0,
      true,
      false,
      false
    )
    schemaVersion.writeMetadata(outBytes, metadata)
    outBytes
  }
}

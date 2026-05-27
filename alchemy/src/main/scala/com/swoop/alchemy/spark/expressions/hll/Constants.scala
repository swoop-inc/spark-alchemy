package com.swoop.alchemy.spark.expressions.hll

object Constants {
  val IMPLEMENTATION_CONFIG_KEY = "com.swoop.alchemy.hll.implementation"

  // impl specific configs
  object Agkn {
    val REG_WIDTH = "com.swoop.alchemy.hll.implementation.agkn.reg_width"
  }

  object StrmLib {
    val SPARSE_PRECISION = "com.swoop.alchemy.hll.implementation.strmlib.sparse_precision"
  }

  object BareBones {
    val REG_WIDTH = "com.swoop.alchemy.hll.implementation.barebones.reg_width"
  }
}

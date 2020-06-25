package org.apache.spark.internal.config

private[spark] object Tests {

  val TEST_USE_COMPRESSED_OOPS_KEY = "spark.test.useCompressedOops"

  val TEST_MEMORY = ConfigBuilder("spark.testing.memory")
    .longConf
    .createWithDefault(Runtime.getRuntime.maxMemory)

  val TEST_SCHEDULE_INTERVAL =
    ConfigBuilder("spark.testing.dynamicAllocation.scheduleInterval")
      .longConf
      .createWithDefault(100)

  val IS_TESTING = ConfigBuilder("spark.testing")
    .booleanConf
    .createOptional

  val TEST_NO_STAGE_RETRY = ConfigBuilder("spark.test.noStageRetry")
    .booleanConf
    .createWithDefault(false)

  val TEST_RESERVED_MEMORY = ConfigBuilder("spark.testing.reservedMemory")
    .longConf
    .createOptional

  val TEST_N_HOSTS = ConfigBuilder("spark.testing.nHosts")
    .intConf
    .createWithDefault(5)

  val TEST_N_EXECUTORS_HOST = ConfigBuilder("spark.testing.nExecutorsPerHost")
    .intConf
    .createWithDefault(4)

  val TEST_N_CORES_EXECUTOR = ConfigBuilder("spark.testing.nCoresPerExecutor")
    .intConf
    .createWithDefault(2)
}

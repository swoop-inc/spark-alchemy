package org.apache.spark.sql.test

import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.scalatest.Suite
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._


/**
  * Helper trait for SQL test suites where all tests share a single [[TestSparkSession]].
  */
trait SharedSparkSessionBase
  extends SQLTestUtils
    with Eventually {
  self: Suite =>

  protected def sparkConf = {
    val conf = new SparkConf()
    conf
      .set(StaticSQLConf.WAREHOUSE_PATH, conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
  }

  /**
    * The [[TestSparkSession]] to use for all tests in this suite.
    *
    * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
    * mode with the default test configurations.
    */
  private var _spark: TestSparkSession = _

  /**
    * The [[TestSparkSession]] to use for all tests in this suite.
    */
  protected implicit def spark: SparkSession = _spark

  /**
    * The [[TestSQLContext]] to use for all tests in this suite.
    */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(sparkConf)
  }

  /**
    * Initialize the [[TestSparkSession]].  Generally, this is just called from
    * beforeAll; however, in test using styles other than FunSuite, there is
    * often code that relies on the session between test group constructs and
    * the actual tests, which may need this session.  It is purely a semantic
    * difference, but semantically, it makes more sense to call
    * 'initializeSession' between a 'describe' and an 'it' call than it does to
    * call 'beforeAll'.
    */
  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSparkSession
    }
  }

  /**
    * Make sure the [[TestSparkSession]] is initialized before any tests are run.
    */
  protected override def beforeAll(): Unit = {
    initializeSession()

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
    * Stop the underlying [[org.apache.spark.SparkContext]], if any.
    */
  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            try {
              waitForTasksToFinish()
            } finally {
              _spark.stop()
              _spark = null
            }
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Clear all persistent datasets after each test
    spark.sharedState.cacheManager.clearCache()
    // files can be closed from other threads, so wait a bit
    // normally this doesn't take more than 1s
    eventually(timeout(30.seconds), interval(2.seconds)) {
      DebugFilesystem.assertNoOpenStreams()
    }
  }
}

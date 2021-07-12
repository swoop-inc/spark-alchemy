package org.apache.spark.sql.test

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.internal.{SQLConf, SessionState, SessionStateBuilder, WithTestConf}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A special `SparkSession` prepared for testing.
  */
class TestSparkSession(sc: SparkContext) extends SparkSession(sc) { self =>
  def this(sparkConf: SparkConf) {
    this(new SparkContext("local[2]", "test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))
  }

  def this() {
    this(new SparkConf)
  }

  SparkSession.setDefaultSession(this)
  SparkSession.setActiveSession(this)

  @transient
  override lazy val sessionState: SessionState = {
    new TestSQLSessionStateBuilder(this, None).build()
  }

  // Needed for extension loading
  def injectExtension(extension: SparkSessionExtensions => Unit): Unit =
    extension(extensions)

  // Needed for Java tests
  def loadTestData(): Unit = {
    testData.loadTestData()
  }

  private object testData extends SQLTestData {
    protected override def spark: SparkSession = self
  }
}


object TestSQLContext {

  /**
    * A map used to store all confs that need to be overridden in sql/core unit tests.
    */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "3"
    )
}

private[sql] class TestSQLSessionStateBuilder(
  session: SparkSession,
  state: Option[SessionState])
  extends SessionStateBuilder(session, state, Map.empty[String, String]) with WithTestConf {
  override def overrideConfs: Map[String, String] = TestSQLContext.overrideConfs
  override def newBuilder: NewBuilder = new TestSQLSessionStateBuilder(_, _)
}

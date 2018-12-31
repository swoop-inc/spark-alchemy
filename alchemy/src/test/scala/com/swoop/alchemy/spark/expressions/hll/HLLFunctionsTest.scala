package com.swoop.alchemy.spark.expressions.hll

import com.swoop.alchemy.spark.expressions.hll.functions.{hll_init_collection, hll_init_collection_agg, _}
import com.swoop.spark.test.HiveSqlSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, lit, map}
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, WordSpec}


object HLLFunctionsTestHelpers {
  System.setSecurityManager(null)

  case class Data(c1: Int, c2: String, c3: Array[Int], c4: Map[String, String], c5: Array[String])

  object Data {
    def apply(c1: Int, c2: String): Data = Data(c1, c2, null, null, null)
  }

  case class Data2(c1: Array[String], c2: Map[String, String])

  case class Data3(c1: String, c2: String, c3: String)

}

class HLLFunctionsTest extends WordSpec with Matchers with HiveSqlSpec {

  import HLLFunctionsTestHelpers._

  lazy val spark = sqlc.sparkSession

  "HyperLogLog functions" should {

    "not allow relativeSD > 39%" in {
      val err = "requirement failed: HLL++ requires at least 4 bits for addressing. Use a lower error, at most 39%."
      val c = lit(null)

      noException should be thrownBy hll_init(c, 0.39)

      the[IllegalArgumentException] thrownBy {
        hll_init(c, 0.40)
      } should have message err

      noException should be thrownBy hll_init_collection(c, 0.39)

      the[IllegalArgumentException] thrownBy {
        hll_init_collection(c, 0.40)
      } should have message err

    }

    "register native org.apache.spark.sql.ext.functions" in {
      HLLFunctionRegistration.registerFunctions(spark)

      noException should be thrownBy spark.sql(
        """select
          |  hll_cardinality(hll_merge(hll_init(1))),
          |  hll_cardinality(hll_merge(hll_init_collection(array(1,2,3)))),
          |  hll_cardinality(hll_init_agg(1)),
          |  hll_cardinality(hll_init_collection_agg(array(1,2,3))),
          |  hll_cardinality(hll_merge(hll_init(1, 0.05))),
          |  hll_cardinality(hll_merge(hll_init_collection(array(1,2,3), 0.05))),
          |  hll_cardinality(hll_init_agg(1, 0.05)),
          |  hll_cardinality(hll_init_collection_agg(array(1,2,3), 0.05)),
          |  hll_cardinality(hll_row_merge(hll_init(1),hll_init(1))),
          |  hll_intersect_cardinality(hll_init(1), hll_init(1))
        """.stripMargin
      )
    }


    "estimate cardinality of simple types and collections" in {
      val a123 = array(lit(1), lit(2), lit(3))

      val simpleValues = Seq(
        lit(null).cast(IntegerType),
        lit(""),
        a123
      ).map(hll_init)

      val collections = Seq(
        lit(null).cast(ArrayType(IntegerType)),
        array(),
        map(),
        a123
      ).map(hll_init_collection)

      val results = cardinalities(spark.range(1).select(simpleValues ++ collections: _*))

      results should be(Seq(
        /* simple types */ 0, 1, 1,
        /* collections */ 0, 0, 0, 3
      ))
    }
    // @todo merge tests with grouping
    "estimate cardinality correctly" in {
      import spark.implicits._

      val df = spark.createDataset[Data](Seq[Data](
        Data(1, "a", Array(1, 2, 3), Map("a" -> "A"), Array.empty),
        Data(2, "b", Array(2, 3, 1), Map("b" -> "B"), Array(null)),
        Data(2, "b", Array(2, 3, 1), Map("b" -> "B"), Array(null, null)),
        Data(3, "c", Array(3, 1, 2), Map("a" -> "A", "b" -> "B"), null),
        Data(2, "b", Array(1, 1, 1), Map("b" -> "B", "c" -> "C"), null),
        Data(3, "c", Array(2, 2, 2), Map("c" -> "C", "a" -> null), null),
        Data(4, "d", null, null, null),
        Data(4, "d", null, null, null),
        Data(5, "e", Array.empty, Map.empty, null),
        Data(5, "e", Array.empty, Map.empty, null)
      ))

      val results = cardinalities(merge(df.select(
        hll_init('c1),
        hll_init('c2),
        hll_init('c3),
        hll_init('c4),
        hll_init('c5),
        hll_init_collection('c3),
        hll_init_collection('c4),
        hll_init_collection('c5)
      )))

      results should be(Seq(
        5, // 5 unique simple values
        5, // 5 unique simple values
        6, // 6 unique arrays (treated as simple types, nulls not counted)
        6, // 6 unique maps (treated as simple types, nulls not counted)
        3, // 3 unique arrays
        3, // 3 unique values across all arrays
        4, // 4 unique (k, v) pairs across all maps
        0 // 0 unique values across all arrays, nulls not counted
      ))
    }
    "estimate multiples correctly" in {
      import spark.implicits._

      val createSampleData =
        spark.createDataset(Seq(
          Data(1, "a"),
          Data(2, "b"),
          Data(2, "b"),
          Data(3, "c"),
          Data(4, "d")
        )).select(hll_init('c1), hll_init('c2))

      val results = cardinalities(merge(createSampleData union createSampleData))

      results should be(Seq(4, 4))
    }
  }

  "HyperLogLog aggregate functions" should {
    // @todo merge tests with grouping
    "estimate cardinality correctly" in {
      import spark.implicits._

      val df = spark.createDataset[Data](Seq[Data](
        Data(1, "a", Array(1, 2, 3), Map("a" -> "A"), Array.empty),
        Data(2, "b", Array(2, 3, 1), Map("b" -> "B"), Array(null)),
        Data(2, "b", Array(2, 3, 1), Map("b" -> "B"), Array(null, null)),
        Data(3, "c", Array(3, 1, 2), Map("a" -> "A", "b" -> "B"), null),
        Data(2, "b", Array(1, 1, 1), Map("b" -> "B", "c" -> "C"), null),
        Data(3, "c", Array(2, 2, 2), Map("c" -> "C", "a" -> null), null),
        Data(4, "d", null, null, null),
        Data(4, "d", null, null, null),
        Data(5, "e", Array.empty, Map.empty, null),
        Data(5, "e", Array.empty, Map.empty, null)
      ))

      val results = cardinalities(df.select(
        hll_init_agg('c1),
        hll_init_agg('c2),
        hll_init_agg('c3),
        hll_init_agg('c4),
        hll_init_agg('c5),
        hll_init_collection_agg('c3),
        hll_init_collection_agg('c4),
        hll_init_collection_agg('c5)
      ))

      results should be(Seq(
        5, // 5 unique simple values
        5, // 5 unique simple values
        6, // 6 unique arrays (treated as simple types, nulls not counted)
        6, // 6 unique maps (treated as simple types, nulls not counted)
        3, // 3 unique arrays
        3, // 3 unique values across all arrays
        4, // 4 unique (k, v) pairs across all maps
        0 // 0 unique values across all arrays, nulls not counted
      ))
    }
    "estimate multiples correctly" in {
      import spark.implicits._

      val createSampleData =
        spark.createDataset(Seq(
          Data(1, "a"),
          Data(2, "b"),
          Data(2, "b"),
          Data(3, "c"),
          Data(4, "d")
        )).select(hll_init_agg('c1), hll_init_agg('c2))

      val results = cardinalities(createSampleData union createSampleData)

      results should be(Seq(4, 4))
    }
  }

  def merge(df: DataFrame): DataFrame =
    df.select(
      df.columns.zipWithIndex.map { case (name, idx) =>
        hll_merge(col(name)).as(s"c$idx")
      }: _*
    )

  def cardinalities(df: DataFrame): Seq[Long] =
    df.select(
      df.columns.zipWithIndex.map { case (name, idx) =>
        hll_cardinality(col(name)).as(s"c$idx")
      }: _*
    ).head.toSeq.map(_.asInstanceOf[Long])

  "HyperLogLog row merge function" should {
    // @todo merge tests with grouping
    "estimate cardinality correctly, with nulls" in {
      import spark.implicits._

      val df = spark.createDataset[Data3](Seq[Data3](
        Data3("a", "a", "a"),
        Data3("a", "b", "c"),
        Data3("a", "b", null),
        Data3("a", null, null),
        Data3(null, null, null)
      ))

      val results = df
        .select(hll_init('c1).as('c1), hll_init('c2).as('c2), hll_init('c3).as('c3))
        .select(hll_cardinality(hll_row_merge('c1, 'c2, 'c3)))
        .na.fill(-1L)
        .as[Long]
        .head(5)
        .toSeq

      results should be(Seq(1, 3, 2, 1, -1)) // nulls skipped
    }
  }

  "HyperLogLog intersection function" should {
    // @todo merge tests with grouping
    "estimate cardinality correctly" in {
      import spark.implicits._

      val df = spark.createDataset[Data3](Seq[Data3](
        Data3("a", "e", "f"),
        Data3("b", "d", "g"),
        Data3("c", "c", "h"),
        Data3("d", "b", "i"),
        Data3("e", "a", "j")
      ))

      val results = df
        .select(hll_init_agg('c1).as('c1), hll_init_agg('c2).as('c2), hll_init_agg('c3).as('c3))
        .select(hll_intersect_cardinality('c1, 'c2), hll_intersect_cardinality('c2, 'c3))
        .as[(Long, Long)]
        .head()

      results should be((5, 0))
    }

    "handle nulls correctly" in {
      import spark.implicits._

      val df = spark.createDataset[Data3](Seq[Data3](
        Data3("a", null, null),
        Data3("b", null, null),
        Data3("c", null, null),
        Data3("d", null, null),
        Data3("e", null, null)
      ))

      val results = df
        .select(hll_init_agg('c1).as('c1), hll_init_agg('c2).as('c2), hll_init_agg('c3).as('c3))
        .select(hll_intersect_cardinality('c1, 'c2), hll_intersect_cardinality('c2, 'c3))
        .na.fill(-1L)
        .as[(Long, Long)]
        .head()

      println(results)
      results should be((0, -1))
    }
  }
}

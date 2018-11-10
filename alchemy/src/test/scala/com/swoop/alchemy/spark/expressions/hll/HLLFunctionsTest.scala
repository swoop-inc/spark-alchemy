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
          |  hll_cardinality(hll_merge(hll_init(array(1,2,3)))),
          |  hll_cardinality(hll_merge(hll_init_collection(array(1,2,3)))),
          |  hll_cardinality(hll_init_agg(array(1,2,3))),
          |  hll_cardinality(hll_init_collection_agg(array(1,2,3))),
          |  hll_cardinality(hll_merge(hll_init(array(1,2,3), 0.05))),
          |  hll_cardinality(hll_merge(hll_init_collection(array(1,2,3), 0.05))),
          |  hll_cardinality(hll_init_agg(array(1,2,3), 0.05)),
          |  hll_cardinality(hll_init_collection_agg(array(1,2,3), 0.05))
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
}

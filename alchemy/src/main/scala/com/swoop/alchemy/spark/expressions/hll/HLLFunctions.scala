package com.swoop.alchemy.spark.expressions.hll

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.swoop.alchemy.spark.expressions.WithHelper
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.{HyperLogLogPlusPlus, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

trait HyperLogLogInit extends Expression {
  def relativeSD: Double

  // This formula for `p` came from org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus:93
  protected[this] val p: Int = Math.ceil(2.0d * Math.log(1.106d / relativeSD) / Math.log(2.0d)).toInt

  require(p >= 4, "HLL++ requires at least 4 bits for addressing. Use a lower error, at most 39%.")

  override def dataType: DataType = BinaryType

  def child: Expression

  def offer(value: Any, buffer: HyperLogLogPlus): HyperLogLogPlus

  def createHll = new HyperLogLogPlus(p, 0)

  def hash(value: Any, dataType: DataType, seed: Long): Long = CardinalityXxHash64Function.hash(value, dataType, seed)

  def hash(value: Any, dataType: DataType): Long = {
    // Using 0L as the seed results in a hash of 0L for empty arrays, which breaks our cardinality estimation tests due
    // to the improbably high number of leading zeros. Instead, use some other arbitrary "normal" long.
    hash(value, dataType, 6705405522910076594L)
  }
}


trait HyperLogLogSimple extends HyperLogLogInit {
  def offer(value: Any, buffer: HyperLogLogPlus): HyperLogLogPlus = {
    buffer.offerHashed(hash(value, child.dataType))
    buffer
  }
}


trait HyperLogLogCollection extends HyperLogLogInit {

  override def checkInputDataTypes(): TypeCheckResult =
    child.dataType match {
      case _: ArrayType | _: MapType | _: NullType => TypeCheckResult.TypeCheckSuccess
      case _ => TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array and map input.")
    }

  def offer(value: Any, buffer: HyperLogLogPlus): HyperLogLogPlus = {
    value match {
      case arr: ArrayData =>
        child.dataType match {
          case ArrayType(et, _) => arr.foreach(et, (_, v) => {
            if (v != null) buffer.offerHashed(hash(v, et))
          })
          case dt => throw new UnsupportedOperationException(s"Unknown DataType for ArrayData: $dt")
        }
      case map: MapData =>
        child.dataType match {
          case MapType(kt, vt, _) => map.foreach(kt, vt, (k, v) => {
            buffer.offerHashed(hash(v, vt, hash(k, kt))) // chain key and value hash
          })
          case dt => throw new UnsupportedOperationException(s"Unknown DataType for MapData: $dt")
        }
      case _: NullType => // do nothing
      case _ => throw new UnsupportedOperationException(s"$prettyName only supports array and map input.")
    }
    buffer
  }
}


trait HyperLogLogInitSingle extends UnaryExpression with HyperLogLogInit with CodegenFallback {
  override def nullable: Boolean = child.nullable

  override def nullSafeEval(value: Any): Any =
    offer(value, createHll).getBytes
}

trait HyperLogLogInitAgg extends NullableSketchAggregation with HyperLogLogInit {

  override def update(buffer: Option[HyperLogLogPlus], inputRow: InternalRow): Option[HyperLogLogPlus] = {
    val value = child.eval(inputRow)
    if (value != null) {
      Some(offer(value, buffer.getOrElse(createHll)))
    } else {
      buffer
    }
  }
}

trait NullableSketchAggregation extends TypedImperativeAggregate[Option[HyperLogLogPlus]] {

  override def createAggregationBuffer(): Option[HyperLogLogPlus] = None

  override def merge(buffer: Option[HyperLogLogPlus], other: Option[HyperLogLogPlus]): Option[HyperLogLogPlus] =
    (buffer, other) match {
      case (Some(a), Some(b)) =>
        a.addAll(b)
        Some(a)
      case (a, None) => a
      case (None, b) => b
      case _ => None
    }

  override def eval(buffer: Option[HyperLogLogPlus]): Any =
    buffer.map(_.getBytes).orNull

  def child: Expression

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = child.nullable

  override def serialize(hll: Option[HyperLogLogPlus]): Array[Byte] =
    hll.map(_.getBytes).orNull

  override def deserialize(bytes: Array[Byte]): Option[HyperLogLogPlus] =
    if (bytes == null) None else Option(HyperLogLogPlus.Builder.build(bytes))
}


/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * This version creates a composable "sketch" for each input row.
  * All expression values treated as simple values.
  *
  * @param child      to estimate the cardinality of.
  * @param relativeSD defines the maximum estimation error allowed
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitSimple(
  override val child: Expression,
  override val relativeSD: Double = 0.05)
  extends HyperLogLogInitSingle with HyperLogLogSimple {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD)
    )
  }

  override def prettyName: String

  = "hll_init"
}


/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * This version combines all input in each aggregate group into a single "sketch".
  * All expression values treated as simple values.
  *
  * @param child      to estimate the cardinality of
  * @param relativeSD defines the maximum estimation error allowed
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitSimpleAgg(
  override val child: Expression,
  override val relativeSD: Double = 0.05,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0)
  extends HyperLogLogInitAgg with HyperLogLogSimple {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): HyperLogLogInitSimpleAgg =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): HyperLogLogInitSimpleAgg =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "hll_init_agg"
}

/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * This version creates a composable "sketch" for each input row.
  * Expression must be is a collection (Array, Map), and collection elements are treated as individual values.
  *
  * @param child      to estimate the cardinality of.
  * @param relativeSD defines the maximum estimation error allowed
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitCollection(
  override val child: Expression,
  override val relativeSD: Double = 0.05)
  extends HyperLogLogInitSingle with HyperLogLogCollection {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD)
    )
  }

  override def prettyName: String = "hll_init_collection"
}


/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * This version combines all input in each aggregate group into a a single "sketch".
  * If `expr` is a collection (Array, Map), collection elements are treated as individual values.
  *
  * @param child      to estimate the cardinality of
  * @param relativeSD defines the maximum estimation error allowed
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitCollectionAgg(
  child: Expression,
  relativeSD: Double = 0.05,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0)
  extends HyperLogLogInitAgg with HyperLogLogCollection {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): HyperLogLogInitCollectionAgg =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): HyperLogLogInitCollectionAgg =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "hll_init_collection_agg"
}


/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * This version aggregates the "sketches" into a single merged "sketch" that represents the union of the constituents.
  *
  * @param child "sketch" to merge
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr) - Returns the merged HLL++ sketch.
  """)
case class HyperLogLogMerge(
  child: Expression,
  override val mutableAggBufferOffset: Int,
  override val inputAggBufferOffset: Int)
  extends NullableSketchAggregation {

  def this(child: Expression) = this(child, 0, 0)

  override def update(buffer: Option[HyperLogLogPlus], inputRow: InternalRow): Option[HyperLogLogPlus] = {
    val value = child.eval(inputRow)
    if (value != null) {
      val hll = value match {
        case b: Array[Byte] => HyperLogLogPlus.Builder.build(b)
        case _ => throw new IllegalStateException(s"$prettyName only supports Array[Byte]")
      }
      buffer.map(_.merge(hll).asInstanceOf[HyperLogLogPlus])
        .orElse(Option(hll))
    } else {
      buffer
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case BinaryType => TypeCheckResult.TypeCheckSuccess
      case _ => TypeCheckResult.TypeCheckFailure(s"$prettyName only supports binary input")
    }
  }

  override def dataType: DataType = BinaryType

  override def withNewMutableAggBufferOffset(newOffset: Int): HyperLogLogMerge =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): HyperLogLogMerge =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "hll_merge"
}

/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * This version merges multiple "sketches" in one row into a single field.
  * @see HyperLogLogMerge
  *
  * @param children "sketch" row fields to merge
  */
@ExpressionDescription(
  usage =
    """
_FUNC_(expr) - Returns the merged HLL++ sketch.
""")
case class HyperLogLogRowMerge(children: Seq[Expression])
  extends Expression with ExpectsInputTypes with CodegenFallback {

  require(children.nonEmpty, s"$prettyName requires at least one argument.")

  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(BinaryType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = children.forall(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val flatInputs = children.flatMap(_.eval(input) match {
      case null => None
      case b: Array[Byte] => Some(HyperLogLogPlus.Builder.build(b))
      case _ => throw new IllegalStateException(s"$prettyName only supports Array[Byte]")
    })

    if (flatInputs.isEmpty) null
    else {
      val acc = flatInputs.head
      flatInputs.tail.foreach(acc.addAll)
      acc.getBytes
    }
  }

  override def prettyName: String = "hll_row_merge"
}

/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * Returns the estimated cardinality of an HLL++ "sketch"
  *
  * @param child HLL++ "sketch"
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(sketch) - Returns the estimated cardinality of the binary representation produced by HyperLogLog++.
  """)
case class HyperLogLogCardinality(override val child: Expression) extends UnaryExpression with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Long = {
    val data = input.asInstanceOf[Array[Byte]]
    HyperLogLogPlus.Builder.build(data).cardinality()
  }

  override def prettyName: String = "hll_cardinality"
}

/**
  * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm.
  *
  * Computes a merged (unioned) sketch and uses the fact that |A intersect B| = (|A| + |B|) - |A union B| to estimate
  * the intersection cardinality of two HLL++ "sketches".
  *
  * @see HyperLogLogRowMerge
  * @see HyperLogLogCardinality
  *
  * @param left  HLL++ "sketch"
  * @param right HLL++ "sketch"
  *
  * @return the estimated intersection cardinality (0 if one sketch is null, but null if both are)
  */
@ExpressionDescription(
  usage =
    """
    _FUNC_(sketchL, sketchR) - Returns the estimated intersection cardinality of the binary representations produced by
    HyperLogLog++. Computes a merged (unioned) sketch and uses the fact that |A intersect B| = (|A| + |B|) - |A union B|.
    Returns null if both sketches are null, but 0 if only one is
  """)
case class HyperLogLogIntersectionCardinality(override val left: Expression, override val right: Expression) extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = LongType

  override def nullable: Boolean = left.nullable && right.nullable

  override def eval(input: InternalRow): Any = {
    val leftValue = left.eval(input)
    val rightValue = right.eval(input)

    if (leftValue != null && rightValue != null) {
      val leftHLL = HyperLogLogPlus.Builder.build(leftValue.asInstanceOf[Array[Byte]])
      val rightHLL = HyperLogLogPlus.Builder.build(rightValue.asInstanceOf[Array[Byte]])

      val leftCount = leftHLL.cardinality()
      val rightCount = rightHLL.cardinality()
      leftHLL.addAll(rightHLL)
      val unionCount = leftHLL.cardinality()

      // guarantee a non-negative result despite the approximate nature of the counts
      math.max((leftCount + rightCount) - unionCount, 0L)
    } else {
      if (leftValue != null || rightValue != null) {
        0L
      } else {
        null
      }
    }
  }

  override def prettyName: String = "hll_intersect_cardinality"
}

object functions extends HLLFunctions

trait HLLFunctions extends WithHelper {

  def hll_init(e: Column, relativeSD: Double): Column = withExpr {
    HyperLogLogInitSimple(e.expr, relativeSD)
  }

  def hll_init(columnName: String, relativeSD: Double): Column =
    hll_init(col(columnName), relativeSD)

  def hll_init(e: Column): Column = withExpr {
    HyperLogLogInitSimple(e.expr)
  }

  def hll_init(columnName: String): Column =
    hll_init(col(columnName))

  def hll_init_collection(e: Column, relativeSD: Double): Column = withExpr {
    HyperLogLogInitCollection(e.expr, relativeSD)
  }

  def hll_init_collection(columnName: String, relativeSD: Double): Column =
    hll_init_collection(col(columnName), relativeSD)

  def hll_init_collection(e: Column): Column = withExpr {
    HyperLogLogInitCollection(e.expr)
  }

  def hll_init_collection(columnName: String): Column =
    hll_init_collection(col(columnName))

  def hll_init_agg(e: Column, relativeSD: Double): Column = withAggregateFunction {
    HyperLogLogInitSimpleAgg(e.expr, relativeSD)
  }

  def hll_init_agg(columnName: String, relativeSD: Double): Column =
    hll_init_agg(col(columnName), relativeSD)

  def hll_init_agg(e: Column): Column = withAggregateFunction {
    HyperLogLogInitSimpleAgg(e.expr)
  }

  def hll_init_agg(columnName: String): Column =
    hll_init_agg(col(columnName))

  def hll_init_collection_agg(e: Column, relativeSD: Double): Column = withAggregateFunction {
    HyperLogLogInitCollectionAgg(e.expr, relativeSD)
  }

  def hll_init_collection_agg(columnName: String, relativeSD: Double): Column =
    hll_init_collection_agg(col(columnName), relativeSD)

  def hll_init_collection_agg(e: Column): Column = withAggregateFunction {
    HyperLogLogInitCollectionAgg(e.expr)
  }

  def hll_init_collection_agg(columnName: String): Column =
    hll_init_collection_agg(col(columnName))

  def hll_merge(e: Column): Column = withAggregateFunction {
    HyperLogLogMerge(e.expr, 0, 0)
  }

  def hll_merge(columnName: String): Column =
    hll_merge(col(columnName))

  def hll_row_merge(es: Column*): Column = withExpr {
    HyperLogLogRowMerge(es.map(_.expr))
  }

  // split arguments to avoid collision w/ above after erasure
  def hll_row_merge(columnName: String, columnNames: String*): Column =
    hll_row_merge((columnName +: columnNames).map(col): _*)

  def hll_cardinality(e: Column): Column = withExpr {
    HyperLogLogCardinality(e.expr)
  }

  def hll_cardinality(columnName: String): Column =
    hll_cardinality(col(columnName))

  def hll_intersect_cardinality(l: Column, r: Column): Column = withExpr {
    HyperLogLogIntersectionCardinality(l.expr, r.expr)
  }

  def hll_intersect_cardinality(leftColumnName: String, rightColumnName: String): Column =
    hll_intersect_cardinality(col(leftColumnName), col(rightColumnName))
}

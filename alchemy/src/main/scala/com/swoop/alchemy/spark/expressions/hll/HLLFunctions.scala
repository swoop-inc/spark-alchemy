package com.swoop.alchemy.spark.expressions.hll

import com.swoop.alchemy.spark.expressions.WithHelper
import com.swoop.alchemy.spark.expressions.hll.HyperLogLogBase.{nameToImpl, resolveImplementation}
import com.swoop.alchemy.spark.expressions.hll.Implementation.{AGGREGATE_KNOWLEDGE, AGKN, STREAM_LIB, STRM}
import org.apache.spark.sql.EncapsulationViolator.createAnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus.validateDoubleLiteral
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionDescription, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SparkSession}

trait HyperLogLogBase {
  def impl: Implementation
}

object HyperLogLogBase {
  def resolveImplementation(exp: Expression): Implementation = exp match {
    case null => resolveImplementation
    case _ => nameToImpl(exp, "last argument")
  }

  def resolveImplementation(exp: String): Implementation = exp match {
    case null => resolveImplementation
    case s => nameToImpl(s)
  }

  def resolveImplementation(implicit impl: Implementation = null): Implementation =
    if (impl != null)
      impl
    else
      SparkSession.getActiveSession
        .flatMap(_.conf.getOption(IMPLEMENTATION_CONFIG_KEY))
        .map(nameToImpl)
        .getOrElse(StreamLib)

  def nameToImpl(name: Expression, argName: String = "argument"): Implementation = name match {
    case Literal(s: Any, StringType) =>
      nameToImpl(s.toString)
    case _ =>
      throw createAnalysisException(
        s"The $argName must be a string argument (${Implementation.OPTIONS.mkString("/")}) designating one of the implementation options."
      )
  }


  def nameToImpl(name: String): Implementation = name match {
    case STRM => StreamLib
    case STREAM_LIB => StreamLib
    case AGKN => AgKn
    case AGGREGATE_KNOWLEDGE => AgKn
    case s => throw createAnalysisException(
      s"The HLL implementation choice '$s' is not one of the valid options: ${Implementation.OPTIONS.mkString(", ")}"
    )
  }
}

trait HyperLogLogInit extends Expression with UnaryLike[Expression] with HyperLogLogBase {
  def relativeSD: Double

  // This formula for `p` came from org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus:93
  protected[this] val p: Int = Math.ceil(2.0d * Math.log(1.106d / relativeSD) / Math.log(2.0d)).toInt

  require(p >= 4, "HLL requires at least 4 bits for addressing. Use a lower error, at most 39%.")

  override def dataType: DataType = BinaryType

  def child: Expression

  def offer(value: Any, buffer: Instance): Instance

  def createHll: Instance = impl.createHll(p)

  def hash(value: Any, dataType: DataType, seed: Long): Long = CardinalityXxHash64Function.hash(value, dataType, seed)

  def hash(value: Any, dataType: DataType): Long = {
    // Using 0L as the seed results in a hash of 0L for empty arrays, which breaks our cardinality estimation tests due
    // to the improbably high number of leading zeros. Instead, use some other arbitrary "normal" long.
    hash(value, dataType, 6705405522910076594L)
  }
}

trait HyperLogLogSimple extends HyperLogLogInit {
  def offer(value: Any, buffer: Instance): Instance = {
    buffer.offer(hash(value, child.dataType))
  }
}

trait HyperLogLogCollection extends HyperLogLogInit {

  override def checkInputDataTypes(): TypeCheckResult =
    child.dataType match {
      case _: ArrayType | _: MapType | _: NullType => TypeCheckResult.TypeCheckSuccess
      case _ => TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array and map input.")
    }

  def offer(value: Any, buffer: Instance): Instance = {
    value match {
      case arr: ArrayData =>
        child.dataType match {
          case ArrayType(et, _) => arr.foreach(et, (_, v) => {
            if (v != null) buffer.offer(hash(v, et))
          })
          case dt => throw new UnsupportedOperationException(s"Unknown DataType for ArrayData: $dt")
        }
      case map: MapData =>
        child.dataType match {
          case MapType(kt, vt, _) => map.foreach(kt, vt, (k, v) => {
            buffer.offer(hash(v, vt, hash(k, kt))) // chain key and value hash
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
    offer(value, createHll).serialize
}

trait HyperLogLogInitAgg extends NullableSketchAggregation with HyperLogLogInit {

  override def update(buffer: Option[Instance], inputRow: InternalRow): Option[Instance] = {
    val value = child.eval(inputRow)
    if (value != null) {
      Some(offer(value, buffer.getOrElse(createHll)))
    } else {
      buffer
    }
  }
}

trait NullableSketchAggregation extends TypedImperativeAggregate[Option[Instance]] with HyperLogLogBase with UnaryLike[Expression] {

  override def createAggregationBuffer(): Option[Instance] = None

  override def merge(buffer: Option[Instance], other: Option[Instance]): Option[Instance] =
    (buffer, other) match {
      case (Some(a), Some(b)) =>
        Some(a.merge(b))
      case (a, None) => a
      case (None, b) => b
      case _ => None
    }

  override def eval(buffer: Option[Instance]): Any =
    buffer.map(_.serialize).orNull

  def child: Expression

  override def nullable: Boolean = child.nullable

  override def serialize(hll: Option[Instance]): Array[Byte] =
    hll.map(_.serialize).orNull

  override def deserialize(bytes: Array[Byte]): Option[Instance] =
    if (bytes == null) None else Option(impl.deserialize(bytes))
}


/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * This function creates a composable "sketch" for each input row.
 * All expression values are treated as simple values.
 *
 * @param child      to estimate the cardinality of.
 * @param relativeSD defines the maximum estimation error allowed
 * @param impl       HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD[, implName]]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitSimple(
  override val child: Expression,
  override val relativeSD: Double = 0.05,
  override val impl: Implementation = resolveImplementation)
  extends HyperLogLogInitSingle with HyperLogLogSimple {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = validateDoubleLiteral(relativeSD)
    )
  }

  def this(child: Expression, relativeSD: Expression, implName: Expression) = {
    this(
      child = child,
      relativeSD = validateDoubleLiteral(relativeSD),
      impl = resolveImplementation(implName)
    )
  }

  override def prettyName: String = "hll_init"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}


/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * This version combines all input in each aggregate group into a single "sketch".
 * All expression values treated as simple values.
 *
 * @param child      to estimate the cardinality of
 * @param relativeSD defines the maximum estimation error allowed
 * @param impl       HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD[, implName]]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitSimpleAgg(
  override val child: Expression,
  override val relativeSD: Double = 0.05,
  override val impl: Implementation = resolveImplementation,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0)
  extends HyperLogLogInitAgg with HyperLogLogSimple {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = validateDoubleLiteral(relativeSD))
  }

  def this(child: Expression, relativeSD: Expression, implName: Expression) = {
    this(
      child = child,
      relativeSD = validateDoubleLiteral(relativeSD),
      impl = resolveImplementation(implName)
    )
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): HyperLogLogInitSimpleAgg =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): HyperLogLogInitSimpleAgg =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "hll_init_agg"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * This version creates a composable "sketch" for each input row.
 * Expression must be is a collection (Array, Map), and collection elements are treated as individual values.
 *
 * @param child      to estimate the cardinality of.
 * @param relativeSD defines the maximum estimation error allowed
 * @param impl       HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD[, implName]]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitCollection(
  override val child: Expression,
  override val relativeSD: Double = 0.05,
  override val impl: Implementation = resolveImplementation)
  extends HyperLogLogInitSingle with HyperLogLogCollection {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = validateDoubleLiteral(relativeSD)
    )
  }

  def this(child: Expression, relativeSD: Expression, implName: Expression) = {
    this(
      child = child,
      relativeSD = validateDoubleLiteral(relativeSD),
      impl = resolveImplementation(implName)
    )
  }


  override def prettyName: String = "hll_init_collection"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}


/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * This version combines all input in each aggregate group into a a single "sketch".
 * If `expr` is a collection (Array, Map), collection elements are treated as individual values.
 *
 * @param child      to estimate the cardinality of
 * @param relativeSD defines the maximum estimation error allowed
 * @param impl       HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, relativeSD[, implName]]) - Returns the composable "sketch" by HyperLogLog++.
      `relativeSD` defines the maximum estimation error allowed.
  """)
case class HyperLogLogInitCollectionAgg(
  override val child: Expression,
  override val relativeSD: Double = 0.05,
  override val impl: Implementation = resolveImplementation,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0)
  extends HyperLogLogInitAgg with HyperLogLogCollection {

  def this(child: Expression) = this(child, relativeSD = 0.05)

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child,
      validateDoubleLiteral(relativeSD)
    )
  }

  def this(child: Expression, relativeSD: Expression, implName: Expression) = {
    this(
      child,
      validateDoubleLiteral(relativeSD),
      resolveImplementation(implName)
    )
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): HyperLogLogInitCollectionAgg =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): HyperLogLogInitCollectionAgg =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "hll_init_collection_agg"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}


/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * This version aggregates the "sketches" into a single merged "sketch" that represents the union of the constituents.
 *
 * @param child "sketch" to merge
 * @param impl  HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, implName]) - Returns the merged HLL sketch.
  """)
case class HyperLogLogMerge(
  child: Expression,
  override val impl: Implementation = resolveImplementation,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0)
  extends NullableSketchAggregation {

  def this(child: Expression) = this(child, resolveImplementation)

  def this(child: Expression, implName: Expression) = this(child, resolveImplementation(implName))

  override def update(buffer: Option[Instance], inputRow: InternalRow): Option[Instance] = {
    val value = child.eval(inputRow)
    if (value != null) {
      val hll = value match {
        case b: Array[Byte] => impl.deserialize(b)
        case _ => throw new IllegalStateException(s"$prettyName only supports Array[Byte]")
      }
      buffer.map(_.merge(hll))
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

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * This version merges multiple "sketches" in one row into a single field.
 *
 * @see [[HyperLogLogMerge]]
 * @param children "sketch" row fields to merge
 * @param impl     HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(expr[, implName]) - Returns the merged HLL sketch.
  """)
case class HyperLogLogRowMerge(
  override val children: Seq[Expression],
  override val impl: Implementation = resolveImplementation)
  extends Expression with ExpectsInputTypes with CodegenFallback with HyperLogLogBase {

  def this(children: Seq[Expression]) = this({
    assert(children.nonEmpty, s"function requires at least one argument")
    children
    }.last match {
    case Literal(_: Any, StringType) => children.init
    case _ => children
  },
    children.last match {
      case Literal(s: Any, StringType) => resolveImplementation(s.toString)
      case _ => resolveImplementation
    }
  )

  require(children.nonEmpty, s"$prettyName requires at least one argument.")

  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(BinaryType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = children.forall(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val flatInputs = children.flatMap(_.eval(input) match {
      case null => None
      case b: Array[Byte] => Some(impl.deserialize(b))
      case _ => throw new IllegalStateException(s"$prettyName only supports Array[Byte]")
    })

    if (flatInputs.isEmpty) null
    else {
      val acc = flatInputs.head
      flatInputs.tail.foreach(acc.merge)
      acc.serialize
    }
  }

  override def prettyName: String = "hll_row_merge"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * Returns the estimated cardinality of an HLL "sketch"
 *
 * @param child HLL "sketch"
 * @param impl  HLL implementation to use
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(sketch[, implName]) - Returns the estimated cardinality of the binary representation produced by HyperLogLog++.
  """)
case class HyperLogLogCardinality(
  override val child: Expression,
  override val impl: Implementation = resolveImplementation
) extends UnaryExpression with ExpectsInputTypes with CodegenFallback with HyperLogLogBase {

  def this(child: Expression) = this(child, resolveImplementation)

  def this(child: Expression, implName: Expression) = this(child, resolveImplementation(implName))

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Long = {
    val data = input.asInstanceOf[Array[Byte]]
    impl.deserialize(data).cardinality
  }

  override def prettyName: String = "hll_cardinality"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * The underlying [[Implementation]] can be changed by setting a [[IMPLEMENTATION_CONFIG_KEY configuration value]]
 * in the [[SparkSession]] to the implementation name, or passing it as an argument.
 *
 * Computes a merged (unioned) sketch and uses the fact that |A intersect B| = (|A| + |B|) - |A union B| to estimate
 * the intersection cardinality of two HLL "sketches".
 *
 * @note Error in the cardinality of the intersection is determined by the cardinality of the constituent sketches, not
 *       the cardinality of the intersection itself (i.e. it may be much larger than naively expected) -
 *       https://research.neustar.biz/2012/12/17/hll-intersections-2/
 *
 * @see HyperLogLogRowMerge
 * @see HyperLogLogCardinality
 * @param left  HLL "sketch"
 * @param right HLL "sketch"
 * @param impl  HLL implementation to use
 * @return the estimated intersection cardinality (0 if one sketch is null, but null if both are)
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(sketchL, sketchR[, implName]) - Returns the estimated intersection cardinality of the binary representations produced by
    HyperLogLog. Computes a merged (unioned) sketch and uses the fact that |A intersect B| = (|A| + |B|) - |A union B|.
    Returns null if both sketches are null, but 0 if only one is
  """)
case class HyperLogLogIntersectionCardinality(
  override val left: Expression,
  override val right: Expression,
  override val impl: Implementation = resolveImplementation
) extends BinaryExpression with ExpectsInputTypes with CodegenFallback with HyperLogLogBase {

  def this(left: Expression, right: Expression) = this(left, right, resolveImplementation)

  def this(left: Expression, right: Expression, implName: Expression) =
    this(left, right, resolveImplementation(implName))

  override def inputTypes: Seq[DataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = LongType

  override def nullable: Boolean = left.nullable && right.nullable

  override def eval(input: InternalRow): Any = {
    val leftValue = left.eval(input)
    val rightValue = right.eval(input)

    if (leftValue != null && rightValue != null) {
      val leftHLL = impl.deserialize(leftValue.asInstanceOf[Array[Byte]])
      val rightHLL = impl.deserialize(rightValue.asInstanceOf[Array[Byte]])

      val leftCount = leftHLL.cardinality
      val rightCount = rightHLL.cardinality
      leftHLL.merge(rightHLL)
      val unionCount = leftHLL.cardinality

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

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}


/**
 * HyperLogLog (HLL) is a state of the art cardinality estimation algorithm with multiple implementations available.
 *
 * This function converts between implementations. Currently the only conversion supported is from the StreamLib
 * implementation (`"STRM"` or `"STREAM_LIB"`) to the Aggregate Knowledge implementation (`"AGKN"` or
 * `"AGGREGATE_KNOWLEDGE"`).
 *
 * @note Converted values CANNOT be merged with unconverted ("native") values of type that they've been converted to.
 *       This is because the different implementations use different parts of the hashed valued to construct the HLL
 *       (effectively equivalent to using different hash functions).
 * @param child HLL "sketch"
 * @param from  string name of implementation type of the given sketch
 * @param to    string name of implementation type to convert the given sketch to
 */

@ExpressionDescription(
  usage =
    """
    _FUNC_(sketch, implNameFrom, implNameTo) - Converts between implementations.
  """)
case class HyperLogLogConvert(
  override val child: Expression,
  from: Implementation,
  to: Implementation
) extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  def this(hll: Expression, fromName: Expression, toName: Expression) = {
    this(hll, nameToImpl(fromName, "second argument"), nameToImpl(toName, "third argument"))
  }


  def this(hll: Expression, fromName: String, toName: String) = {
    this(hll, nameToImpl(fromName), nameToImpl(toName))
  }

  override def dataType: DataType = BinaryType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def nullSafeEval(hll: Any): Any = (from, to) match {
    case (StreamLib, AgKn) => strmToAgkn(hll.asInstanceOf[Array[Byte]])
    case _ => throw new IllegalArgumentException(
      "HLL conversion is currently only supported from STREAM_LIB to AGGREGATE_KNOWLEDGE"
    )
  }

  override def prettyName: String = "hll_convert"

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

object functions extends HLLFunctions {
  val impl: Implementation = null
}

trait HLLFunctions extends WithHelper {

  implicit def impl: Implementation

  def hll_init(e: Column, relativeSD: Double, implName: String = null): Column = withExpr {
    HyperLogLogInitSimple(e.expr, relativeSD, resolveImplementation(implName))
  }

  def hll_init(columnName: String, relativeSD: Double): Column =
    hll_init(col(columnName), relativeSD)

  def hll_init(columnName: String, relativeSD: Double, implName: String): Column =
    hll_init(col(columnName), relativeSD, implName)

  def hll_init(e: Column): Column = withExpr {
    HyperLogLogInitSimple(e.expr, impl = resolveImplementation)
  }

  def hll_init(columnName: String): Column =
    hll_init(col(columnName))

  def hll_init_collection(e: Column, relativeSD: Double, implName: String = null): Column = withExpr {
    HyperLogLogInitCollection(e.expr, relativeSD, resolveImplementation(implName))
  }

  def hll_init_collection(columnName: String, relativeSD: Double): Column =
    hll_init_collection(col(columnName), relativeSD)

  def hll_init_collection(columnName: String, relativeSD: Double, implName: String): Column =
    hll_init_collection(col(columnName), relativeSD, implName)

  def hll_init_collection(e: Column): Column = withExpr {
    HyperLogLogInitCollection(e.expr, impl = resolveImplementation)
  }

  def hll_init_collection(columnName: String): Column =
    hll_init_collection(col(columnName))

  def hll_init_agg(e: Column, relativeSD: Double, implName: String = null): Column = withAggregateFunction {
    HyperLogLogInitSimpleAgg(e.expr, relativeSD, resolveImplementation(implName))
  }

  def hll_init_agg(columnName: String, relativeSD: Double): Column =
    hll_init_agg(col(columnName), relativeSD)

  def hll_init_agg(columnName: String, relativeSD: Double, implName: String): Column =
    hll_init_agg(col(columnName), relativeSD, implName)

  def hll_init_agg(e: Column): Column = withAggregateFunction {
    HyperLogLogInitSimpleAgg(e.expr, impl = resolveImplementation)
  }

  def hll_init_agg(columnName: String): Column =
    hll_init_agg(col(columnName))

  def hll_init_collection_agg(e: Column, relativeSD: Double, implName: String = null): Column = withAggregateFunction {
    HyperLogLogInitCollectionAgg(e.expr, relativeSD, resolveImplementation(implName))
  }

  def hll_init_collection_agg(columnName: String, relativeSD: Double): Column =
    hll_init_collection_agg(col(columnName), relativeSD)

  def hll_init_collection_agg(columnName: String, relativeSD: Double, implName: String): Column =
    hll_init_collection_agg(col(columnName), relativeSD, implName)

  def hll_init_collection_agg(e: Column): Column = withAggregateFunction {
    HyperLogLogInitCollectionAgg(e.expr, impl = resolveImplementation)
  }

  def hll_init_collection_agg(columnName: String): Column =
    hll_init_collection_agg(col(columnName))

  def hll_merge(e: Column, implName: String = null): Column = withAggregateFunction {
    HyperLogLogMerge(e.expr, resolveImplementation(implName))
  }

  def hll_merge(columnName: String): Column =
    hll_merge(col(columnName))

  def hll_merge(columnName: String, implName: String): Column =
    hll_merge(col(columnName), implName)

  def hll_row_merge(es: Column*): Column = withExpr {
    HyperLogLogRowMerge(es.map(_.expr), resolveImplementation)
  }

  def hll_row_merge(implName: String, es: Column*): Column = withExpr {
    HyperLogLogRowMerge(es.map(_.expr), resolveImplementation(implName))
  }

  def hll_cardinality(e: Column, implName: String = null): Column = withExpr {
    HyperLogLogCardinality(e.expr, resolveImplementation(implName))
  }

  def hll_cardinality(columnName: String): Column =
    hll_cardinality(col(columnName))

  def hll_cardinality(columnName: String, implName: String): Column =
    hll_cardinality(col(columnName), implName)

  def hll_intersect_cardinality(l: Column, r: Column, implName: String = null): Column = withExpr {
    HyperLogLogIntersectionCardinality(l.expr, r.expr, resolveImplementation(implName))
  }

  def hll_intersect_cardinality(leftColumnName: String, rightColumnName: String): Column =
    hll_intersect_cardinality(col(leftColumnName), col(rightColumnName))

  def hll_intersect_cardinality(leftColumnName: String, rightColumnName: String, implName: String): Column =
    hll_intersect_cardinality(col(leftColumnName), col(rightColumnName), implName)

  def hll_convert(hll: Column, from: String, to: String): Column = withExpr {
    HyperLogLogConvert(hll.expr, nameToImpl(from), nameToImpl(to))
  }

  def hll_convert(columnName: String, from: String, to: String): Column =
    hll_convert(col(columnName), from, to)
}

object HLLFunctions {
  def withImpl(hllImpl: Implementation): HLLFunctions = new HLLFunctions {
    override implicit def impl: Implementation = hllImpl
  }
}

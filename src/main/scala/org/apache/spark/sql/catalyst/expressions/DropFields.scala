package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 *
 * Drops one or more fields in struct.
 * Returns null if struct is null.
 * Returns empty struct if all fields in struct are dropped.
 * This is a no-op if schema doesn't contain given field name(s).
 * If there are multiple fields with one of the given names, they will all be dropped.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, name1, name2, ...) - Drops one or more fields in struct.",
  examples =
    """
    Examples:
      > SELECT _FUNC_({"a":1,"b":2}, "b");
       {"a":1}
  """)
// scalastyle:on line.size.limit
case class DropFields(children: Seq[Expression]) extends Expression {

  private lazy val struct: Expression = children.head

  private lazy val fieldNameExprs = children.drop(1)

  private lazy val fieldNames = fieldNameExprs.map(_.eval().asInstanceOf[UTF8String].toString)

  private lazy val ogStructType: StructType =
    struct.dataType.asInstanceOf[StructType]

  private lazy val indicesToKeep = {
    ogStructType.fieldNames.zipWithIndex.collect {
      case (fieldName, idx) if !fieldNames.contains(fieldName) => idx
    }
  }

  override lazy val dataType: StructType = {
    StructType(ogStructType.zipWithIndex.collect { case (field, idx) if indicesToKeep.contains(idx) => field })
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size < 1) {
      return TypeCheckResult.TypeCheckFailure(s"$prettyName expects at least 1 argument.")
    }

    val typeName = struct.dataType.typeName
    val expectedStructType = StructType(Nil).typeName
    if (typeName != expectedStructType) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only $expectedStructType is allowed to appear at first position, got: $typeName.")
    }

    if (fieldNameExprs.contains(null) || fieldNameExprs.exists(e => !(e.foldable && e.dataType == StringType))) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only non-null foldable ${StringType.catalogString} expressions are allowed after first position.")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def nullable: Boolean = struct.nullable

  override def eval(input: InternalRow): Any = {
    val structValue = struct.eval(input)
    if (structValue == null) {
      null
    } else {
      val ogValues = structValue.asInstanceOf[InternalRow].toSeq(ogStructType)
      val newValues = ogValues.zipWithIndex.collect { case (ogValue, idx) if indicesToKeep.contains(idx) => ogValue }
      InternalRow.fromSeq(newValues)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val structGen = struct.genCode(ctx)
    val resultCode: String = {
      val structVar = structGen.value
      val rowClass = classOf[GenericInternalRow].getName
      val rowValuesVar = ctx.freshName("rowValues")

      val populateRowValuesVar = ogStructType.fields
        .zipWithIndex
        .collect { case (field, idx) if indicesToKeep.contains(idx) => (field, idx) }
        .zipWithIndex
        .map { case ((structField, ogIdx), newIdx) =>
          val nullCheck = s"$structVar.isNullAt($ogIdx)"
          val nonNullValue = CodeGenerator.getValue(structVar, structField.dataType, ogIdx.toString)

          s"""
             |if ($nullCheck) {
             | $rowValuesVar[$newIdx] = null;
             |} else {
             | $rowValuesVar[$newIdx] = $nonNullValue;
             |}""".stripMargin
        }
        .mkString("\n|")

      s"""
         |Object[] $rowValuesVar = new Object[${dataType.length}];
         |
         |$populateRowValuesVar
         |
         |${ev.value} = new $rowClass($rowValuesVar);
         |""".stripMargin

    }

    if (nullable) {
      val nullSafeEval =
        structGen.code + ctx.nullSafeExec(struct.nullable, structGen.isNull) {
          s"""
             |${ev.isNull} = false; // resultCode could change nullability.
             |$resultCode
             |""".stripMargin
        }

      ev.copy(code =
        code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          $nullSafeEval
          """)
    } else {
      ev.copy(code =
        code"""
          ${structGen.code}
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          $resultCode
          """, isNull = FalseLiteral)
    }
  }

  override def prettyName: String = "drop_fields"
}

object DropFields {
  @deprecated("use DropFields(children: Seq[Expression]) constructor.", "0.2.4")
  def apply(struct: Expression, fieldNames: String*): DropFields =
    DropFields(struct +: fieldNames.map(Literal(_)))
}

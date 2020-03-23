package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String


/**
 *
 * Renames StructFields in StructType.
 * Returns null if struct is null.
 * This is a no-op if schema doesn't contain any fields with existingFieldNames.
 * If there are multiple fields with existingFieldName, they will all be renamed.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, nameA, renamedA, nameB, renamedB, ...) - Renames StructField in StructType.",
  examples =
    """
    Examples:
      > SELECT _FUNC_({"a":1,"b":2}, "b", "c");
       {"a":1, "c":2}
  """)
// scalastyle:on line.size.limit
case class RenameFields(children: Seq[Expression]) extends Expression {

  private lazy val struct: Expression = children.head
  private lazy val nameExprs: Seq[Expression] = children.drop(1)

  override def nullable: Boolean = struct.nullable

  override lazy val dataType: StructType = {
    var renamedFields = struct.dataType.asInstanceOf[StructType].fields
    nameExprs.grouped(2).foreach { case Seq(existingNameExpr, newNameExpr) =>
      val existingName = existingNameExpr.eval().asInstanceOf[UTF8String].toString
      val newName = newNameExpr.eval().asInstanceOf[UTF8String].toString
      renamedFields = renamedFields.map {
        case field if field.name == existingName => field.copy(name = newName)
        case field => field
      }
    }

    StructType(renamedFields)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 == 0) {
      return TypeCheckResult.TypeCheckFailure(s"$prettyName expects an odd number of arguments.")
    }

    val typeName = struct.dataType.typeName
    val expectedStructType = StructType(Nil).typeName
    if (typeName != expectedStructType) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only $expectedStructType is allowed to appear at first position, got: $typeName.")
    }

    if (nameExprs.exists(e => e == null || e.eval() == null || !(e.foldable && e.dataType == StringType))) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only non-null foldable ${StringType.catalogString} expressions are allowed to appear after first position.")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def eval(input: InternalRow): Any = struct.eval(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val structGen = struct.genCode(ctx)
    val structVar = structGen.value
    val resultCode: String = s"${ev.value} = $structVar;"

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

  override def prettyName: String = "rename_fields"
}

object RenameFields {
  @deprecated("use RenameFields(children: Seq[Expression]) constructor.", "0.2.4")
  def apply(struct: Expression, existingFieldName: String, newFieldName: String): RenameFields =
    RenameFields(struct, Seq(existingFieldName), Seq(newFieldName))

  @deprecated("use RenameFields(children: Seq[Expression]) constructor.", "0.2.4")
  def apply(struct: Expression, existingFieldNames: Seq[String], newFieldNames: Seq[String]): RenameFields =
    RenameFields(struct +: existingFieldNames.zip(newFieldNames).flatMap { case (existingName, newName) =>
      Seq(Literal(existingName), Literal(newName))
    })
}

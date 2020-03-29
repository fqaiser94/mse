package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Adds/replaces fields in a struct.
 * Returns null if struct is null.
 * If multiple fields already exist with the one of the given fieldNames, they will all be replaced.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, name1, val1, name2, val2, ...) - Adds/replaces fields in struct by name.",
  examples = """
    Examples:
      > SELECT _FUNC_({"a":1}, "b", 2, "c", 3);
       {"a":1,"b":2,"c":3}
  """)
// scalastyle:on line.size.limit
case class AddFields(children: Seq[Expression]) extends Expression {

  private lazy val struct: Expression = children.head
  private lazy val (nameExprs, valExprs) = children.drop(1).grouped(2).map {
    case Seq(name, value) => (name, value)
  }.toList.unzip
  private lazy val fieldNames = nameExprs.map(_.eval().asInstanceOf[UTF8String].toString)
  private lazy val pairs = fieldNames.zip(valExprs)

  override def nullable: Boolean = struct.nullable

  private lazy val ogStructType: StructType =
    struct.dataType.asInstanceOf[StructType]

  override lazy val dataType: StructType = {
    val existingFields = ogStructType.fields.map { x => (x.name, x) }
    val addOrReplaceFields = pairs.map { case (fieldName, field) =>
      (fieldName, StructField(fieldName, field.dataType, field.nullable))
    }
    val newFields = loop(existingFields, addOrReplaceFields).map(_._2)
    StructType(newFields)
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

    if (nameExprs.contains(null) || nameExprs.exists(e => !(e.foldable && e.dataType == StringType))) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only non-null foldable ${StringType.catalogString} expressions are allowed to appear at even position.")
    }

    if (valExprs.contains(null)) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only non-null expressions are allowed to appear at odd positions after first position.")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def eval(input: InternalRow): Any = {
    val structValue = struct.eval(input)
    if (structValue == null) {
      null
    } else {
      val existingValues: Seq[(FieldName, Any)] =
        ogStructType.fieldNames.zip(structValue.asInstanceOf[InternalRow].toSeq(ogStructType))
      val addOrReplaceValues: Seq[(FieldName, Any)] =
        pairs.map { case (fieldName, expression) => (fieldName, expression.eval(input)) }
      val newValues = loop(existingValues, addOrReplaceValues).map(_._2)
      InternalRow.fromSeq(newValues)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val structGen = struct.genCode(ctx)
    val addOrReplaceFieldsGens = valExprs.map(_.genCode(ctx))
    val resultCode: String = {
      val structVar = structGen.value
      type NullCheck = String
      type NonNullValue = String
      val existingFieldsCode: Seq[(FieldName, (NullCheck, NonNullValue))] =
        ogStructType.fields.zipWithIndex.map {
          case (structField, i) =>
            val nullCheck = s"$structVar.isNullAt($i)"
            val nonNullValue = CodeGenerator.getValue(structVar, structField.dataType, i.toString)
            (structField.name, (nullCheck, nonNullValue))
        }
      val addOrReplaceFieldsCode: Seq[(FieldName, (NullCheck, NonNullValue))] =
        fieldNames.zip(addOrReplaceFieldsGens).map {
          case (fieldName, fieldExprCode) =>
            val nullCheck = fieldExprCode.isNull.code
            val nonNullValue = fieldExprCode.value.code
            (fieldName, (nullCheck, nonNullValue))
        }
      val newFieldsCode = loop(existingFieldsCode, addOrReplaceFieldsCode)
      val rowClass = classOf[GenericInternalRow].getName
      val rowValuesVar = ctx.freshName("rowValues")
      val populateRowValuesVar = newFieldsCode.zipWithIndex.map {
        case ((_, (nullCheck, nonNullValue)), i) =>
          s"""
             |if ($nullCheck) {
             | $rowValuesVar[$i] = null;
             |} else {
             | $rowValuesVar[$i] = $nonNullValue;
             |}""".stripMargin
      }.mkString("\n|")

      s"""
         |Object[] $rowValuesVar = new Object[${dataType.length}];
         |
         |${addOrReplaceFieldsGens.map(_.code).mkString("\n")}
         |$populateRowValuesVar
         |
         |${ev.value} = new $rowClass($rowValuesVar);
          """.stripMargin
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

  override def prettyName: String = "add_fields"

  private type FieldName = String

  /**
   * Recursively loop through addOrReplaceFields, adding or replacing fields by FieldName.
   */
  @scala.annotation.tailrec
  private def loop[V](existingFields: Seq[(String, V)],
                      addOrReplaceFields: Seq[(String, V)]): Seq[(String, V)] = {
    if (addOrReplaceFields.nonEmpty) {
      val existingFieldNames = existingFields.map(_._1)
      val newField@(newFieldName, _) = addOrReplaceFields.head

      if (existingFieldNames.contains(newFieldName)) {
        loop(
          existingFields.map {
            case (fieldName, _) if fieldName == newFieldName => newField
            case x => x
          },
          addOrReplaceFields.drop(1))
      } else {
        loop(
          existingFields :+ newField,
          addOrReplaceFields.drop(1))
      }
    } else {
      existingFields
    }
  }
}

object AddFields {
  @deprecated("use AddFields(children: Seq[Expression]) constructor.", "0.2.4")
  def apply(struct: Expression, fieldName: String, fieldExpression: Expression): AddFields =
    AddFields(struct :: Literal(fieldName) :: fieldExpression :: Nil)

  @deprecated("use AddFields(children: Seq[Expression]) constructor.", "0.2.4")
  def apply(struct: Expression, fieldNames: Seq[String], fieldExpressions: Seq[Expression]): AddFields = {
    val exprs = fieldNames.zip(fieldExpressions).flatMap { case (name, expr) => Seq(Literal(name), expr) }
    AddFields(struct +: exprs)
  }
}
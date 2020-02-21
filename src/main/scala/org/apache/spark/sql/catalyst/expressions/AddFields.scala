package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{StructField, StructType}

/**
  *
  * Adds/replaces a field in a struct.
  * Returns null if struct is null.
  *
  * @param struct           : The struct to add field to.
  * @param fieldNames       : The names to give the fields to add to given struct.
  * @param fieldExpressions : The expressions to assign to each fieldName in fieldNames.
  */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, fieldName, field) - Adds/replaces field in given struct.",
  examples =
    """
    Examples:
      > SELECT _FUNC_({"a":1}, "b", 2);
       {"a":1,"b":2}
  """)
// scalastyle:on line.size.limit
// TODO: test for fieldNames and fieldExpressions must be same length
case class AddFields(struct: Expression, fieldNames: Seq[String], fieldExpressions: Seq[Expression]) extends Expression {

  private type FieldName = String

  /**
    * Recursively loops through addOrReplaceFields, adding or replacing fields by FieldName.
    */
  private def loop[V](existingFields: Seq[(FieldName, V)], addOrReplaceFields: Seq[(FieldName, V)]): Seq[(FieldName, V)] = {
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

  private lazy val ogStructType: StructType =
    struct.dataType.asInstanceOf[StructType]

  private val pairs = fieldNames.zip(fieldExpressions)

  override def children: Seq[Expression] = struct +: fieldExpressions

  override lazy val dataType: StructType = {
    val existingFields = ogStructType.fields.map { x => (x.name, x) }
    val addOrReplaceFields = pairs.map { case (fieldName, field) => (fieldName, StructField(fieldName, field.dataType, field.nullable)) }
    val newFields = loop(existingFields, addOrReplaceFields).map(_._2)
    StructType(newFields)
  }

  override def nullable: Boolean = struct.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    // check struct is Struct DataType
    val typeName = struct.dataType.typeName
    if (typeName != StructType(Nil).typeName) {
      return TypeCheckResult.TypeCheckFailure(
        s"struct should be struct data type. struct is $typeName")
    }

    if (fieldNames.contains(null)) {
      return TypeCheckResult.TypeCheckFailure("fieldName cannot be null")
    }

    // TODO: add test for this
    if (fieldExpressions.contains(null)) {
      return TypeCheckResult.TypeCheckFailure("fieldExpressions cannot be null")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def eval(input: InternalRow): Any = {
    val structValue = struct.eval(input)
    if (structValue == null) {
      null
    } else {
      val existingValues: Seq[(FieldName, Any)] = ogStructType.fieldNames.zip(structValue.asInstanceOf[InternalRow].toSeq(ogStructType))
      val addOrReplaceValues: Seq[(FieldName, Any)] = pairs.map { case (fieldName, expression) => (fieldName, expression.eval(input)) }
      val newValues = loop(existingValues, addOrReplaceValues).map(_._2)
      InternalRow.fromSeq(newValues)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val structGen = struct.genCode(ctx)
    val addOrReplaceFieldsGens = fieldExpressions.map(_.genCode(ctx))
    val resultCode: String = {
      val structVar = structGen.value
      type NullCheck = String
      type NonNullValue = String
      val existingFields: Seq[(FieldName, (NullCheck, NonNullValue))] = ogStructType.fields.zipWithIndex.map {
        case (structField, i) =>
          val nullCheck = s"$structVar.isNullAt($i)"
          val nonNullValue = CodeGenerator.getValue(structVar, structField.dataType, i.toString)
          (structField.name, (nullCheck, nonNullValue))
      }
      val addOrReplaceFields: Seq[(FieldName, (NullCheck, NonNullValue))] = fieldNames.zip(addOrReplaceFieldsGens).map {
        case (fieldName, fieldExprCode) =>
          val nullCheck = fieldExprCode.isNull.code
          val nonNullValue = fieldExprCode.value.code
          (fieldName, (nullCheck, nonNullValue))
      }
      val newFields = loop(existingFields, addOrReplaceFields)
      val rowClass = classOf[GenericInternalRow].getName
      val rowValuesVar = ctx.freshName("rowValues")
      val populateRowValuesVar = newFields.zipWithIndex.map {
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
}

object AddFields {
  def apply(struct: Expression, fieldName: String, fieldExpression: Expression): AddFields =
    AddFields(struct, Seq(fieldName), Seq(fieldExpression))
}
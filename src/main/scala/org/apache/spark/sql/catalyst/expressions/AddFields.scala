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
// TODO: need a test for what to do in case where multiple fields with the same name exist and that is the field user wants to replace
//  what does withColumn do in this scenario? it replaces all columns with that name
// TODO: test for fieldNames and fieldExpressions must be same length
// TODO: tests adding multiple fields.
case class AddFields(struct: Expression, fieldNames: Seq[String], fieldExpressions: Seq[Expression]) extends Expression {

  private lazy val ogStructType: StructType =
    struct.dataType.asInstanceOf[StructType]

  private val pairs = fieldNames.zip(fieldExpressions)

  override def children: Seq[Expression] = struct +: fieldExpressions

  override lazy val dataType: StructType = {
    def loop(existingFields: Seq[StructField], addOrReplaceFields: Seq[StructField]): Seq[StructField] = {
      if (addOrReplaceFields.nonEmpty) {
        val existingFieldNames = existingFields.map(_.name)
        val newField@StructField(newFieldName, _, _, _) = addOrReplaceFields.head

        if (existingFieldNames.contains(newFieldName)) {
          loop(
            existingFields.map {
              case StructField(fieldName, _, _, _) if fieldName == newFieldName => newField
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

    val existingFields: Seq[StructField] = ogStructType.fields
    val addOrReplaceFields = pairs.map { case (fieldName, field) => StructField(fieldName, field.dataType, field.nullable) }
    val newFields = loop(existingFields, addOrReplaceFields)
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
      def loop(existingFields: Seq[(String, Any)], addOrReplaceFields: Seq[(String, Any)]): Seq[(String, Any)] = {
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

      val existingFields = ogStructType.fieldNames.zip(structValue.asInstanceOf[InternalRow].toSeq(ogStructType))
      val addOrReplaceFields = pairs.map { case (fieldName, expression) => (fieldName, expression.eval(input)) }
      val newValues = loop(existingFields, addOrReplaceFields).map(_._2)
      InternalRow.fromSeq(newValues)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val rowValuesVar = ctx.freshName("rowValues")

    val fieldName = fieldNames.head
    val field = fieldExpressions.head

    val fieldEval = field.genCode(ctx)

    val ogStructType: StructType = struct.dataType.asInstanceOf[StructType]
    val idxToPatch = {
      val idx = ogStructType.fieldNames.indexOf(fieldName)
      if (idx == -1) ogStructType.length else idx
    }

    val left = struct
    val right = field
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val f: (String, String) => String = (structVar, _) => {
      val populateRowValuesVar = dataType.fields.zipWithIndex
        .map {
          case (structField, i) =>
            val (nullCheck, nonNullValue) = if (i == idxToPatch) {
              (fieldEval.isNull, fieldEval.value)
            } else {
              Tuple2(
                s"$structVar.isNullAt($i)",
                CodeGenerator.getValue(structVar, structField.dataType, i.toString))
            }

            s"""
               |if ($nullCheck) {
               | $rowValuesVar[$i] = null;
               |} else {
               | $rowValuesVar[$i] = $nonNullValue;
               |}""".stripMargin
        }
        .mkString("\n|")

      s"""
         |${fieldEval.code}
         |Object[] $rowValuesVar = new Object[${dataType.length}];
         |
             |$populateRowValuesVar
         |
             |${ev.value} = new $rowClass($rowValuesVar);
          """.stripMargin
    }
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
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
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }

  override def prettyName: String = "add_fields"
}

object AddFields {
  def apply(struct: Expression, fieldName: String, field: Expression): AddFields =
    AddFields(struct, Seq(fieldName), Seq(field))
}
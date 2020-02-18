package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.StructType

/**
  *
  * Adds/replaces a field in a struct.
  * Returns null if struct is null.
  *
  * @param struct    : The struct to add field to.
  * @param fieldName : The name to give the field to add to given struct.
  * @param field     : The value to assign to fieldName.
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
case class AddFields(struct: Expression, fieldNames: Seq[String], fieldExpressions: Seq[Expression]) extends Expression {

  private def createNamedStruct = {
    val newFields: Seq[Expression] = {
      def loop(existingFields: Seq[(String, Expression)], newFields: Seq[(String, Expression)]): Seq[Expression] = {
        if (newFields.nonEmpty) {
          val existingFieldNames = existingFields.map(_._1)
          val newField@(newFieldName, _) = newFields.head

          if (existingFieldNames.contains(newFieldName)) {
            loop(
              existingFields.map {
                case (fieldName, _) if fieldName == newFieldName => newField
                case x => x
              },
              newFields.drop(1))
          } else {
            loop(
              existingFields :+ newField,
              newFields.drop(1))
          }
        } else {
          existingFields.flatMap {
            case (fieldName, expression) => Seq(Literal(fieldName), expression)
          }
        }
      }

      val existingFields: Seq[(String, Expression)] = struct.dataType.asInstanceOf[StructType].fields.zipWithIndex.map { case (field, i) => (field.name, GetStructField(struct, i)) }
      val newFields = fieldNames.zip(fieldExpressions)
      loop(existingFields, newFields)
    }

    val result = CreateNamedStruct(newFields)
    If(IsNull(struct), Literal.create(null, result.dataType), result)
  }

  override val children: Seq[Expression] = createNamedStruct.children

  override lazy val dataType: StructType = createNamedStruct.dataType.asInstanceOf[StructType]

  override def nullable: Boolean = createNamedStruct.nullable

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

    createNamedStruct.checkInputDataTypes()
  }

  override def eval(input: InternalRow): Any = createNamedStruct.eval(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = createNamedStruct.doGenCode(ctx, ev)

  override def prettyName: String = "add_fields"
}

object AddFields {
  def apply(struct: Expression, fieldName: String, field: Expression): AddFields =
    AddFields(struct, Seq(fieldName), Seq(field))
}
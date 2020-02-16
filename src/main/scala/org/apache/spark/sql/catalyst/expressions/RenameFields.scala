package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.StructType

/**
  *
  * Renames StructField in StructType.
  * Returns null if struct is null.
  * This is a no-op if schema doesn't contain any field with existingFieldName.
  * If there are multiple fields with existingFieldName, they will all be renamed.
  *
  * @param struct             : The struct to rename field in.
  * @param existingFieldNames : The name of the fields to rename.
  * @param newFieldNames      : The name to give the fields you want to rename.
  */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, fieldName) - Renames StructField in StructType.",
  examples =
    """
    Examples:
      > SELECT _FUNC_({"a":1,"b":2}, "b", "c");
       {"a":1, "c":2}
  """)
// scalastyle:on line.size.limit
case class RenameFields(struct: Expression, existingFieldNames: Seq[String], newFieldNames: Seq[String])
  extends UnaryExpression {

  override lazy val dataType: StructType = {
    val renamedFields = {
      var fields = struct.dataType.asInstanceOf[StructType].fields
      existingFieldNames.zip(newFieldNames).foreach { case (existingName, newName) =>
        fields = fields.map {
          case field if field.name == existingName => field.copy(name = newName)
          case field => field
        }
      }
      fields
    }

    StructType(renamedFields)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    // check that existingFieldNames contain the same number of elements as newFieldNames
    val existingFieldNamesLength = existingFieldNames.length
    val newFieldNamesLength = newFieldNames.length
    if (existingFieldNamesLength != newFieldNamesLength) {
      return TypeCheckResult.TypeCheckFailure(
        s"existingFieldNames should contain same number of elements as newFieldNames. existingFieldNames is of length $existingFieldNamesLength. newFieldNames is of length $newFieldNamesLength.")
    }

    // check struct is Struct DataType
    val typeName = struct.dataType.typeName
    if (typeName != StructType(Nil).typeName) {
      return TypeCheckResult.TypeCheckFailure(
        s"struct should be struct data type. struct is $typeName")
    }

    // check existingFieldName is not null
    if (existingFieldNames.contains(null)) {
      return TypeCheckResult.TypeCheckFailure("existingFieldName cannot be null")
    }

    // check newFieldName is not null
    if (newFieldNames.contains(null)) {
      return TypeCheckResult.TypeCheckFailure("newFieldName cannot be null")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def nullable: Boolean = child.nullable

  override def child: Expression = struct

  override def eval(input: InternalRow): Any = {
    struct.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, structVar => s"${ev.value} = $structVar;")
  }

  override def prettyName: String = "rename_fields"
}

object RenameFields {
  def apply(struct: Expression, existingFieldName: String, newFieldName: String): RenameFields =
    RenameFields(struct, Seq(existingFieldName), Seq(newFieldName))
}

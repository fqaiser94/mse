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
  *
  * @param struct            : The struct to drop fields in.
  * @param existingFieldName : The name of the fields to drop.
  * @param newFieldName      : The name of the fields to drop.
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
case class RenameField(struct: Expression, existingFieldName: String, newFieldName: String)
  extends UnaryExpression {

  override lazy val dataType: StructType = {
    StructType(struct.dataType.asInstanceOf[StructType].map(field =>
      if (field.name == existingFieldName) field.copy(name = newFieldName) else field
    ))
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    // check struct is Struct DataType
    val typeName = struct.dataType.typeName
    if (typeName != StructType(Nil).typeName) {
      return TypeCheckResult.TypeCheckFailure(
        s"struct should be struct data type. struct is $typeName")
    }

    // check existingFieldName is not null
    if (existingFieldName == null) {
      return TypeCheckResult.TypeCheckFailure("existingFieldName cannot be null")
    }

    // check newFieldName is not null
    if (newFieldName == null) {
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

  override def prettyName: String = "rename_field"
}

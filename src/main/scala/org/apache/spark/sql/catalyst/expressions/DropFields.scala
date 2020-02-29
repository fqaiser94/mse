package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.StructType

/**
  *
  * Drops one or more StructFields from StructType.
  * Returns null if struct is null.
  * Returns empty struct if all fields in struct are dropped.
  * This is a no-op if schema doesn't contain given field name(s).
  * If there are multiple fields with one of the fieldNames, they will all be dropped.
  *
  * @param struct     : The struct to drop fields in.
  * @param fieldNames : The names of the fields to drop.
  */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, fieldName) - Drops one or more StructFields from StructType.",
  examples =
    """
    Examples:
      > SELECT _FUNC_({"a":1,"b":2}, "b");
       {"a":1}
  """)
// scalastyle:on line.size.limit
case class DropFields(struct: Expression, fieldNames: String*)
  extends UnaryExpression {

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
    // check struct is Struct DataType
    val typeName = struct.dataType.typeName
    if (typeName != StructType(Nil).typeName) {
      return TypeCheckResult.TypeCheckFailure(
        s"struct should be struct data type. struct is $typeName")
    }

    // check none of the given field names is null
    if (fieldNames.contains(null)) {
      return TypeCheckResult.TypeCheckFailure("fieldNames cannot contain null")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def nullable: Boolean = child.nullable

  override def child: Expression = struct

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
    val rowClass = classOf[GenericInternalRow].getName
    val rowValuesVar = ctx.freshName("rowValues")

    nullSafeCodeGen(
      ctx,
      ev,
      structVar => {
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
      })
  }

  override def prettyName: String = "drop_fields"
}
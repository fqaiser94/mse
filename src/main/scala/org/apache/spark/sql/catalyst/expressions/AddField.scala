package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{StructField, StructType}

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
case class AddField(struct: Expression, fieldName: String, field: Expression)
  extends BinaryExpression {

  override lazy val dataType: StructType = {
    val newStructField = StructField(fieldName, field.dataType, field.nullable)
    StructType(ogStructType.fields.patch(idxToPatch, Seq(newStructField), 1))
  }
  private lazy val ogStructType: StructType =
    struct.dataType.asInstanceOf[StructType]
  private lazy val idxToPatch = {
    val idx = ogStructType.fieldNames.indexOf(fieldName)
    if (idx == -1) ogStructType.length else idx
  }

  override def right: Expression = field

  override def nullable: Boolean = left.nullable

  override def left: Expression = struct

  override def checkInputDataTypes(): TypeCheckResult = {
    // check struct is Struct DataType
    val typeName = struct.dataType.typeName
    if (typeName != StructType(Nil).typeName) {
      return TypeCheckResult.TypeCheckFailure(
        s"struct should be struct data type. struct is $typeName")
    }

    if (fieldName == null) {
      return TypeCheckResult.TypeCheckFailure("fieldName cannot be null")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def eval(input: InternalRow): Any = {
    val structValue = struct.eval(input)
    if (structValue == null) {
      null
    } else {
      val ogValues = structValue.asInstanceOf[InternalRow].toSeq(ogStructType)
      val newValues = ogValues.patch(idxToPatch, field.eval(input) :: Nil, 1)
      InternalRow.fromSeq(newValues)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val rowValuesVar = ctx.freshName("rowValues")
    val fieldEval = field.genCode(ctx)

    nullSafeCodeGen(
      ctx,
      ev,
      (structVar, _) => {
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
      })
  }

  override def prettyName: String = "add_field"
}
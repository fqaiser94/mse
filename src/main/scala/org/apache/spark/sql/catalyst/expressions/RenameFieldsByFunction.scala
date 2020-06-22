package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.types.AbstractDataType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


/**
 *
 * Renames StructFields in StructType according to a given function. Existing column names are mapped with this
 * function to produce new names.
 * Returns null if struct is null.
 * This is a no-op if all the schema's field names are not changed by calling the function on them.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, renameFunc) - Renames StructFields in StructType, using a given function. This function" +
          "should accept a string and return a string.",
  examples =
    """
    Examples:
      > SELECT _FUNC_({"a":1,"b":2}, colName -> concat(colName, "_suffix"));
       {"a_suffix":1, "b_suffix":2}
  """)
// scalastyle:on line.size.limit
case class RenameFieldsByFunction(
  argument: Expression,
  function: Expression) extends SimpleHigherOrderFunction {

  @transient lazy val StructType(fields) = argument.dataType

  @transient lazy val LambdaFunction(
  _, (colNameVar: NamedLambdaVariable) :: Nil, _) = function

  private lazy val struct: Expression = argument
  override def nullable: Boolean = struct.nullable

  override def argumentType: AbstractDataType = StructType

  override def dataType: StructType = {
    val origFields = struct.dataType.asInstanceOf[StructType].fields
    val renamedFields = origFields.map (col => {
      colNameVar.value.set(UTF8String.fromString(col.name))
      // the function does not need a non-null inputRow parameter, since it operates only on the
      // variable bound above
      val result = functionForEval.eval(null).asInstanceOf[UTF8String].toString
      if (result == col.name) col else col.copy(name = result)
    })

    StructType(renamedFields)
  }

  override def functionType: AbstractDataType = StringType

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): RenameFieldsByFunction = {
    copy(function = f(function, (StringType, false) :: Nil))
  }

  @transient lazy val LambdaFunction(_, Seq(elementVar: NamedLambdaVariable), _) = function

  override def nullSafeEval(inputRow: InternalRow, argumentValue: Any): Any = {
    struct.eval(inputRow)
  }

  override def prettyName: String = "rename_fields_by_function"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    //TODO: fill this in?
    null
  }
}



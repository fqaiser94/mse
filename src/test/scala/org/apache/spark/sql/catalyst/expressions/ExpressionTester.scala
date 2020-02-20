package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.test.ExamplePointUDT
import org.apache.spark.sql.types._

trait ExpressionTester extends SparkFunSuite with ExpressionEvalHelper {

  val differentDataTypes: Seq[DataType] = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType.USER_DEFAULT,
    DateType,
    TimestampType,
    StringType,
    BinaryType,
    CalendarIntervalType,
    ArrayType(StringType),
    MapType(StringType, StringType),
    StructType(StructField("a", IntegerType) :: Nil),
    new ExamplePointUDT)
  val nonNullInputs: Seq[Literal] = differentDataTypes.map(Literal.default)
  val nullInputs: Seq[Literal] = differentDataTypes.map(Literal.create(null, _))

  def checkEvaluationCustom(expression: => Expression,
                      expected: Any,
                      expectedDataType: DataType,
                      inputRow: InternalRow = EmptyRow): Unit = {
    checkEvaluation(expression = expression, expected = expected, inputRow = inputRow)
    assert(expression.dataType == expectedDataType)
  }

  def create_unsafe_row(fieldTypes: Array[DataType], fieldValues: Array[Any]): UnsafeRow = {
    val converter = UnsafeProjection.create(fieldTypes)
    val row = new SpecificInternalRow(fieldTypes)
    fieldValues.zipWithIndex.foreach { case (value, idx) =>
      row.update(idx, value)
    }
    val unsafeRow: UnsafeRow = converter.apply(row)
    unsafeRow
  }

  def createArray(values: Any*): ArrayData = new GenericArrayData(values.toArray)

}

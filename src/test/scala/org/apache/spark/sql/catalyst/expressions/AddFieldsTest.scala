package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class AddFieldsTest extends ExpressionTester {

  val (nonNullStruct, nullStruct, unsafeRowStruct) = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", ArrayType(StringType)),
      StructField("c", BooleanType)))

    val fieldTypes = schema.fields.map(_.dataType)
    val fieldValues = Array(1, Seq("hello"), true)
    val unsafeFieldValues = Array(1, createArray(UTF8String.fromString("hello")), true)

    Tuple3(
      Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema),
      Literal.create(create_unsafe_row(fieldTypes, unsafeFieldValues), schema))
  }

  test("prettyName should return \"add_field\"") {
    assert(AddFields(nullStruct, "a", Literal(2)).prettyName == "add_field")
  }

  test("checkInputDataTypes should fail if struct is not a struct dataType") {
    nonNullInputs
      .foreach {
        case inputStruct if inputStruct.dataType.typeName == "struct" =>
          val result = AddFields(inputStruct, "b", Literal(2)).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckSuccess
          assert(result == expected)
        case inputStruct =>
          val result = AddFields(inputStruct, "b", Literal(2)).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckFailure(
            s"struct should be struct data type. struct is ${inputStruct.expr.dataType.typeName}")
          assert(result == expected)
      }
  }

  test("checkInputDataTypes should fail if fieldName = null") {
    assert({
      val result = AddFields(nonNullStruct, null, Literal.create(2, IntegerType)).checkInputDataTypes()
      val expected = TypeCheckResult.TypeCheckFailure("fieldName cannot be null")
      result == expected
    })
  }

  test("should return null if struct = null") {
    checkEvaluation(AddFields(nullStruct, "b", Literal(2)), null)
  }

  test("should add new non-null field to end of struct") {
    nonNullInputs.foreach { inputField =>
      checkEvaluation(
        AddFields(nonNullStruct, "d", inputField),
        create_row(1, Seq("hello"), true, inputField.value))
    }
  }

  test("should add new null field to end of struct") {
    nullInputs.foreach { inputField =>
      checkEvaluation(
        AddFields(nonNullStruct, "d", inputField),
        create_row(1, Seq("hello"), true, null))
    }
  }

  test("should replace field in-place with non-null value in struct") {
    nonNullInputs.foreach { inputField =>
      checkEvaluation(
        AddFields(nonNullStruct, "b", inputField),
        create_row(1, inputField.value, true))
    }
  }

  test("should replace field in-place with null value in struct") {
    nullInputs.foreach { inputField =>
      checkEvaluation(
        AddFields(nonNullStruct, "b", inputField),
        create_row(1, null, true))
    }
  }

  test("should return any null fields in struct during add and replace") {
    differentDataTypes.foreach { dataType =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", dataType)))
      val struct = Literal.create(create_row(1, Literal.create(null, dataType).value), schema)

      checkEvaluation(
        AddFields(struct, "c", Literal.create(1, IntegerType)),
        create_row(1, null, 1))

      checkEvaluation(
        AddFields(struct, "a", Literal.create(1, IntegerType)),
        create_row(1, null))
    }
  }

  test("should be able to handle attribute references during add and replace") {
    nonNullInputs.foreach { literalValue =>
      val value = literalValue.value
      val row = create_row(value, nonNullStruct)
      val attributeReference = AttributeReference("a", literalValue.dataType)().at(0)

      checkEvaluation(
        AddFields(nonNullStruct, "d", attributeReference),
        create_row(1, Seq("hello"), true, value),
        row)

      checkEvaluation(
        AddFields(nonNullStruct, "b", attributeReference),
        create_row(1, value, true),
        row)
    }
  }

  test("should add new field to end of struct of UnsafeRow type") {
    checkEvaluation(
      AddFields(unsafeRowStruct, "d", Literal.create(2)),
      create_row(1, Seq("hello"), true, 2))
  }

  test("should replace field in struct of UnsafeRow type") {
    checkEvaluation(
      AddFields(unsafeRowStruct, "b", Literal.create(2)),
      create_row(1, 2, true))
  }

}

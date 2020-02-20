package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class AddFieldsTest extends ExpressionTester {

  // TODO: all tests should verify schema

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

  test("prettyName should return \"add_fields\"") {
    assert(AddFields(nullStruct, "a", Literal(2)).prettyName == "add_fields")
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
    checkEvaluation(
      AddFields(nullStruct, "b", Literal(2)),
      null)
  }

  test("should add new non-null field to end of struct") {
    nonNullInputs.foreach { inputField =>
      val expectedValue = create_row(1, Seq("hello"), true, inputField.value)

      checkEvaluation(
        AddFields(nonNullStruct, "d", inputField),
        expectedValue)

      checkEvaluation(
        AddFields(unsafeRowStruct, "d", inputField),
        expectedValue)
    }
  }

  test("should add new null field to end of struct") {
    nullInputs.foreach { inputField =>
      val expectedValue = create_row(1, Seq("hello"), true, null)
      checkEvaluation(
        AddFields(nonNullStruct, "d", inputField),
        expectedValue)

      checkEvaluation(
        AddFields(unsafeRowStruct, "d", inputField),
        expectedValue)
    }
  }

  test("should replace field in-place with non-null value in struct") {
    nonNullInputs.foreach { inputField =>
      val expectedValue = create_row(1, inputField.value, true)

      checkEvaluation(
        AddFields(nonNullStruct, "b", inputField),
        expectedValue)

      checkEvaluation(
        AddFields(unsafeRowStruct, "b", inputField),
        expectedValue)
    }
  }

  test("should replace field in-place with null value in struct") {
    nullInputs.foreach { inputField =>
      val expectedValue = create_row(1, null, true)

      checkEvaluation(
        AddFields(nonNullStruct, "b", inputField),
        expectedValue)

      checkEvaluation(
        AddFields(unsafeRowStruct, "b", inputField),
        expectedValue)
    }
  }

  test("should return any null fields in struct during add operation") {
    differentDataTypes.foreach { dataType =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", dataType)))
      val struct = Literal.create(create_row(1, Literal.create(null, dataType).value), schema)
      val unsafeRowStruct = Literal.create(create_unsafe_row(schema.fields.map(_.dataType), Array(1, null)), schema)

      checkEvaluation(
        AddFields(struct, "c", Literal.create(1, IntegerType)),
        create_row(1, null, 1))

      checkEvaluation(
        AddFields(unsafeRowStruct, "c", Literal.create(1, IntegerType)),
        create_row(1, null, 1))
    }
  }

  test("should return any null fields in struct during replace operation") {
    differentDataTypes.foreach { dataType =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", dataType)))
      val struct = Literal.create(create_row(1, Literal.create(null, dataType).value), schema)
      val unsafeRowStruct = Literal.create(create_unsafe_row(schema.fields.map(_.dataType), Array(1, null)), schema)
      val expectedValue = create_row(2, null)

      checkEvaluation(
        AddFields(struct, "a", Literal.create(2, IntegerType)),
        expectedValue)

      checkEvaluation(
        AddFields(unsafeRowStruct, "a", Literal.create(2, IntegerType)),
        expectedValue)
    }
  }

  test("should be able to handle attribute references during add operation") {
    nonNullInputs.foreach { literalValue =>
      val value = literalValue.value
      val attributeReference = AttributeReference("a", literalValue.dataType)().at(0)
      val expectedValue = create_row(1, Seq("hello"), true, value)

      checkEvaluation(
        AddFields(nonNullStruct, "d", attributeReference),
        expectedValue,
        create_row(value, nonNullStruct))

      checkEvaluation(
        AddFields(unsafeRowStruct, "d", attributeReference),
        expectedValue,
        create_row(value, unsafeRowStruct))
    }
  }

  test("should be able to handle attribute references during replace operation") {
    nonNullInputs.foreach { literalValue =>
      val value = literalValue.value
      val attributeReference = AttributeReference("a", literalValue.dataType)().at(0)
      val expectedValue = create_row(1, value, true)

      checkEvaluation(
        AddFields(nonNullStruct, "b", attributeReference),
        expectedValue,
        create_row(value, nonNullStruct))

      checkEvaluation(
        AddFields(unsafeRowStruct, "b", attributeReference),
        expectedValue,
        create_row(value, unsafeRowStruct))
    }
  }

  test("should add and replace multiple fields") {
    val fieldNames = Seq("c", "d")
    val fieldExpressions = Seq(Literal.create(2), Literal.create(3))
    val expectedValue = create_row(1, Seq("hello"), 2, 3)

    checkEvaluation(
      AddFields(nonNullStruct, fieldNames, fieldExpressions),
      expectedValue)

    checkEvaluation(
      AddFields(unsafeRowStruct, fieldNames, fieldExpressions),
      expectedValue)
  }

  test("should add and replace multiple fields in the order provided") {
    val fieldNames = Seq("c", "c", "d", "d")
    val fieldExpressions = Seq(Literal.create(2), Literal.create(3), Literal.create(4), Literal.create(5))
    val expectedValue = create_row(1, Seq("hello"), 3, 5)

    checkEvaluation(
      AddFields(nonNullStruct, fieldNames, fieldExpressions),
      expectedValue)

    checkEvaluation(
      AddFields(unsafeRowStruct, fieldNames, fieldExpressions),
      expectedValue)
  }

  test("should be able to handle attribute references during add and replace of multiple fields") {
    // TODO:
  }

}

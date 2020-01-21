package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class AddFieldTest extends ExpressionTester {

  test("AddField") {
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

    // prettyName should return "add_field"
    assert(AddField(nullStruct, "a", Literal(2)).prettyName == "add_field")

    // checkInputDataTypes should fail if struct is not a struct dataType
    nonNullInputs
      .foreach {
        case inputStruct if inputStruct.dataType.typeName == "struct" =>
          val result = AddField(inputStruct, "b", Literal(2)).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckSuccess
          assert(result == expected)
        case inputStruct =>
          val result = AddField(inputStruct, "b", Literal(2)).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckFailure(
            s"struct should be struct data type. struct is ${inputStruct.expr.dataType.typeName}")
          assert(result == expected)
      }

    // checkInputDataTypes should fail if fieldName = null
    assert({
      val result = AddField(nonNullStruct, null, Literal.create(2, IntegerType)).checkInputDataTypes()
      val expected = TypeCheckResult.TypeCheckFailure("fieldName cannot be null")
      result == expected
    })

    // should return null if struct = null
    checkEvaluation(AddField(nullStruct, "b", Literal(2)), null)

    // should add new field to end of struct
    nonNullInputs.foreach { inputField =>
      checkEvaluation(
        AddField(nonNullStruct, "d", inputField),
        create_row(1, Seq("hello"), true, inputField.value))
    }

    nullInputs.foreach { inputField =>
      checkEvaluation(
        AddField(nonNullStruct, "d", inputField),
        create_row(1, Seq("hello"), true, null))
    }

    // should replace field in-place in struct
    nonNullInputs.foreach { inputField =>
      checkEvaluation(
        AddField(nonNullStruct, "b", inputField),
        create_row(1, inputField.value, true))
    }

    nullInputs.foreach { inputField =>
      checkEvaluation(
        AddField(nonNullStruct, "b", inputField),
        create_row(1, null, true))
    }

    // should return any null fields in struct during add and replace
    differentDataTypes.foreach { dataType =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", dataType)))
      val struct = Literal.create(create_row(1, Literal.create(null, dataType).value), schema)

      checkEvaluation(
        AddField(struct, "c", Literal.create(1, IntegerType)),
        create_row(1, null, 1))

      checkEvaluation(
        AddField(struct, "a", Literal.create(1, IntegerType)),
        create_row(1, null))
    }

    // should be able to handle attribute references during add and replace
    nonNullInputs.foreach { literalValue =>
      val value = literalValue.value
      val row = create_row(value, nonNullStruct)
      val attributeReference = AttributeReference("a", literalValue.dataType)().at(0)

      checkEvaluation(
        AddField(nonNullStruct, "d", attributeReference),
        create_row(1, Seq("hello"), true, value),
        row)

      checkEvaluation(
        AddField(nonNullStruct, "b", attributeReference),
        create_row(1, value, true),
        row)
    }

    // should add new field to end of struct of UnsafeRow type
    checkEvaluation(
      AddField(unsafeRowStruct, "d", Literal.create(2)),
      create_row(1, Seq("hello"), true, 2))

    // should replace field in struct of UnsafeRow type
    checkEvaluation(
      AddField(unsafeRowStruct, "b", Literal.create(2)),
      create_row(1, 2, true))
  }

}

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class AddFieldsTest extends ExpressionTester {

  private val schema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", ArrayType(StringType)),
    StructField("c", BooleanType)))

  private val (nonNullStruct, nullStruct, unsafeRowStruct) = {
    val fieldTypes = schema.fields.map(_.dataType)
    val fieldValues = Array(1, Seq("hello"), true)
    val unsafeFieldValues = Array(1, create_array(UTF8String.fromString("hello")), true)

    Tuple3(
      Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema),
      Literal.create(create_unsafe_row(fieldTypes, unsafeFieldValues), schema))
  }

  private val testNamePrefix = "AddFields: "

  test(testNamePrefix + "prettyName should return \"add_fields\"") {
    assert(
      AddFields(nullStruct :: Literal("a") :: Literal(2) :: Nil).prettyName == "add_fields")
  }

  test(testNamePrefix + "checkInputDataTypes should fail if struct is not a struct dataType") {
    nonNullInputs
      .foreach {
        case inputStruct if inputStruct.dataType.typeName == "struct" =>
          val result =
            AddFields(inputStruct :: Literal("b") :: Literal(2) :: Nil).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckSuccess
          assert(result == expected)
        case inputStruct =>
          val result =
            AddFields(inputStruct :: Literal("b") :: Literal(2) :: Nil).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckFailure(
            s"Only ${StructType(Nil).typeName} is allowed to appear at first position, " +
              s"got: ${inputStruct.expr.dataType.typeName}.")
          assert(result == expected)
      }
  }

  test(testNamePrefix + "should fail if fieldName is null") {
    assert({
      val result = AddFields(nonNullStruct :: null :: Literal.create(2, IntegerType) :: Nil).checkInputDataTypes()
      val expected = TypeCheckResult.TypeCheckFailure(
        s"Only non-null foldable ${StringType.catalogString} expressions are allowed to appear at even position.")
      result == expected
    })
  }

  test(testNamePrefix + "should fail if fieldName is not a string") {
    nonNullInputs.foreach {
      case inputField if inputField.dataType.typeName != StringType.typeName =>
        assert({
          val result = AddFields(nonNullStruct :: inputField :: Literal.create(2, IntegerType) :: Nil).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckFailure(
            s"Only non-null foldable ${StringType.catalogString} expressions are allowed to appear at even position.")
          result == expected
        })
      case inputField =>
        assert({
          val result = AddFields(nonNullStruct :: inputField :: Literal.create(2, IntegerType) :: Nil).checkInputDataTypes()
          val expected = TypeCheckResult.TypeCheckSuccess
          result == expected
        })
    }
  }

  test(testNamePrefix + "should fail if fieldExpression is null") {
    assert({
      val result = AddFields(nonNullStruct :: Literal("a") :: null :: Nil).checkInputDataTypes()
      val expected = TypeCheckResult.TypeCheckFailure(
        s"Only non-null expressions are allowed to appear at odd positions after first position.")
      result == expected
    })
  }

  test(testNamePrefix +
    "checkInputDataTypes should fail if fieldNames and fieldExpressions have different lengths") {
    assert({
      val result = AddFields(nonNullStruct :: Literal("a") :: Nil).checkInputDataTypes()
      val expected = TypeCheckResult.TypeCheckFailure(
        s"add_fields expects an odd number of arguments.")
      result == expected
    })
  }

  test(testNamePrefix +
    "checkInputDataTypes should fail if given an empty list of expressions") {
    assert({
      val result = AddFields(Nil).checkInputDataTypes()
      val expected = TypeCheckResult.TypeCheckFailure(
        s"add_fields expects an odd number of arguments.")
      result == expected
    })
  }

  test(testNamePrefix + "should return null if struct = null") {
    checkEvaluationCustom(
      AddFields(nullStruct :: Literal("d") :: Literal(2) :: Nil),
      null,
      schema.add("d", IntegerType, nullable = false))
  }

  test(testNamePrefix + "should return original struct") {
    checkEvaluationCustom(
      AddFields(nonNullStruct :: Nil),
      nonNullStruct.value,
      schema)

    checkEvaluationCustom(
      AddFields(unsafeRowStruct :: Nil),
      unsafeRowStruct.value,
      schema)
  }

  test(testNamePrefix + "should add new non-null field to end of struct") {
    nonNullInputs.foreach { inputField =>
      val expectedValue = create_row(1, "hello" :: Nil, true, inputField.value)
      val expectedSchema = schema.add("d", inputField.dataType, inputField.nullable)

      checkEvaluationCustom(
        AddFields(nonNullStruct :: Literal("d") :: inputField :: Nil),
        expectedValue,
        expectedSchema)

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("d") :: inputField :: Nil),
        expectedValue,
        expectedSchema)
    }
  }

  test(testNamePrefix + "should add new null field to end of struct") {
    nullInputs.foreach { inputField =>
      val expectedValue = create_row(1, "hello" :: Nil, true, null)
      val expectedSchema = schema.add("d", inputField.dataType, inputField.nullable)

      checkEvaluationCustom(
        AddFields(nonNullStruct :: Literal("d") :: inputField :: Nil),
        expectedValue,
        expectedSchema)

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("d") :: inputField :: Nil),
        expectedValue,
        expectedSchema)
    }
  }

  test(testNamePrefix + "should replace field in-place with non-null value in struct") {
    nonNullInputs.foreach { inputField =>
      val expectedValue = create_row(1, inputField.value, true)
      val expectedSchema = StructType(
        schema.updated(1, StructField("b", inputField.dataType, inputField.nullable)))

      checkEvaluationCustom(
        AddFields(nonNullStruct :: Literal("b") :: inputField :: Nil),
        expectedValue,
        expectedSchema)

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("b") :: inputField :: Nil),
        expectedValue,
        expectedSchema)
    }
  }

  test(testNamePrefix + "should replace field in-place with null value in struct") {
    nullInputs.foreach { inputField =>
      val expectedValue = create_row(1, null, true)
      val expectedSchema = StructType(
        schema.updated(1, StructField("b", inputField.dataType, inputField.nullable)))

      checkEvaluationCustom(
        AddFields(nonNullStruct :: Literal("b") :: inputField :: Nil),
        expectedValue,
        expectedSchema)

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("b") :: inputField :: Nil),
        expectedValue,
        expectedSchema)
    }
  }

  test(testNamePrefix + "should return any null fields in struct during add operation") {
    differentDataTypes.foreach { dataType =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", dataType)))
      val struct = Literal.create(create_row(1, Literal.create(null, dataType).value), schema)
      val unsafeRowStruct = Literal.create(
        create_unsafe_row(schema.fields.map(_.dataType), Array(1, null)), schema)
      val expectedValue = create_row(1, null, 1)
      val expectedSchema = schema.add("c", IntegerType, nullable = false)

      checkEvaluationCustom(
        AddFields(struct :: Literal("c") :: Literal.create(1, IntegerType) :: Nil),
        expectedValue,
        expectedSchema)

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("c") :: Literal.create(1, IntegerType) :: Nil),
        expectedValue,
        expectedSchema)
    }
  }

  test(testNamePrefix + "should return any null fields in struct during replace operation") {
    differentDataTypes.foreach { dataType =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", dataType)))
      val struct = Literal.create(create_row(1, Literal.create(null, dataType).value), schema)
      val unsafeRowStruct = Literal.create(
        create_unsafe_row(schema.fields.map(_.dataType), Array(1, null)), schema)
      val expectedValue = create_row(2, null)
      val expectedSchema = StructType(
        schema.updated(0, StructField("a", IntegerType, nullable = false)))

      checkEvaluationCustom(
        AddFields(struct :: Literal("a") :: Literal.create(2, IntegerType) :: Nil),
        expectedValue,
        expectedSchema)

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("a") :: Literal.create(2, IntegerType) :: Nil),
        expectedValue,
        expectedSchema)
    }
  }

  test(testNamePrefix + "should be able to handle attribute references during add operation") {
    nonNullInputs.foreach { literalValue =>
      val value = literalValue.value
      val attributeReference =
        AttributeReference("a", literalValue.dataType, literalValue.nullable)().at(0)
      val expectedValue = create_row(1, Seq("hello"), true, value)
      val expectedSchema = schema.add("d", literalValue.dataType, literalValue.nullable)

      checkEvaluationCustom(
        AddFields(nonNullStruct :: Literal("d") :: attributeReference :: Nil),
        expectedValue,
        expectedSchema,
        create_row(value, nonNullStruct))

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("d") :: attributeReference :: Nil),
        expectedValue,
        expectedSchema,
        create_row(value, unsafeRowStruct))
    }
  }

  test(testNamePrefix +
    "should be able to handle attribute references during replace operation") {
    nonNullInputs.foreach { literalValue =>
      val value = literalValue.value
      val attributeReference =
        AttributeReference("a", literalValue.dataType, literalValue.nullable)().at(0)
      val expectedValue = create_row(1, value, true)
      val expectedSchema = StructType(
        schema.updated(1, StructField("b", literalValue.dataType, literalValue.nullable)))

      checkEvaluationCustom(
        AddFields(nonNullStruct :: Literal("b") :: attributeReference :: Nil),
        expectedValue,
        expectedSchema,
        create_row(value, nonNullStruct))

      checkEvaluationCustom(
        AddFields(unsafeRowStruct :: Literal("b") :: attributeReference :: Nil),
        expectedValue,
        expectedSchema,
        create_row(value, unsafeRowStruct))
    }
  }

  test(testNamePrefix + "should add and replace multiple fields") {
    val expectedValue = create_row(1, Seq("hello"), 2, 3)
    val expectedSchema =
      StructType(schema.updated(2, StructField("c", IntegerType, nullable = false)))
        .add(StructField("d", IntegerType, nullable = false))

    checkEvaluationCustom(
      AddFields(nonNullStruct :: Literal("c") :: Literal(2) :: Literal("d") :: Literal(3) :: Nil),
      expectedValue,
      expectedSchema)

    checkEvaluationCustom(
      AddFields(unsafeRowStruct :: Literal("c") :: Literal(2) :: Literal("d") :: Literal(3) ::
        Nil),
      expectedValue,
      expectedSchema)
  }

  test(testNamePrefix + "should add and replace multiple fields in the order provided") {
    val expectedValue = create_row(1, Seq("hello"), 3, 5)
    val expectedSchema =
      StructType(schema.updated(2, StructField("c", IntegerType, nullable = false)))
        .add(StructField("d", IntegerType, nullable = false))

    checkEvaluationCustom(
      AddFields(nonNullStruct ::
        Literal("c") :: Literal(2) :: Literal("c") :: Literal(3) ::
        Literal("d") :: Literal(4) :: Literal("d") :: Literal(5) ::
        Nil),
      expectedValue,
      expectedSchema)

    checkEvaluationCustom(
      AddFields(unsafeRowStruct ::
        Literal("c") :: Literal(2) :: Literal("c") :: Literal(3) ::
        Literal("d") :: Literal(4) :: Literal("d") :: Literal(5) ::
        Nil),
      expectedValue,
      expectedSchema)
  }

  test(testNamePrefix +
    "should handle attribute references during add operation of multiple fields") {
    val literalValue = Literal.create(1, IntegerType)
    val value = literalValue.value
    val attributeReference =
      AttributeReference("a", literalValue.dataType, literalValue.nullable)().at(0)

    val expectedValue = create_row(1, Seq("hello"), true, 1, 1)
    val expectedSchema = schema.add(StructField("d", IntegerType, nullable = false))
      .add(StructField("e", IntegerType, nullable = false))

    checkEvaluationCustom(
      AddFields(nonNullStruct ::
        Literal("d") :: attributeReference ::
        Literal("e") :: attributeReference :: Nil),
      expectedValue,
      expectedSchema,
      create_row(value, nonNullStruct))

    checkEvaluationCustom(
      AddFields(unsafeRowStruct ::
        Literal("d") :: attributeReference ::
        Literal("e") :: attributeReference :: Nil),
      expectedValue,
      expectedSchema,
      create_row(value, unsafeRowStruct))
  }

  test(testNamePrefix +
    "should handle attribute references during replace operation of multiple fields") {
    val literalValue = Literal.create(2, IntegerType)
    val value = literalValue.value
    val attributeReference =
      AttributeReference("a", literalValue.dataType, literalValue.nullable)().at(0)

    val expectedValue = create_row(1, 2, 2)
    val expectedSchema = StructType(schema
      .updated(1, StructField("b", IntegerType, nullable = false))
      .updated(2, StructField("c", IntegerType, nullable = false)))

    checkEvaluationCustom(
      AddFields(nonNullStruct ::
        Literal("b") :: attributeReference ::
        Literal("c") :: attributeReference :: Nil),
      expectedValue,
      expectedSchema,
      create_row(value, nonNullStruct))

    checkEvaluationCustom(
      AddFields(unsafeRowStruct ::
        Literal("b") :: attributeReference ::
        Literal("c") :: attributeReference :: Nil),
      expectedValue,
      expectedSchema,
      create_row(value, unsafeRowStruct))
  }

  test(testNamePrefix + "should replace all fields with given name") {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("a", BooleanType)))

    val (nonNullStruct, unsafeRowStruct) = {
      val fieldTypes = schema.fields.map(_.dataType)
      val fieldValues = Array(1, true)
      val unsafeFieldValues = Array[Any](1, true)

      Tuple2(
        Literal.create(create_row(fieldValues: _*), schema),
        Literal.create(create_unsafe_row(fieldTypes, unsafeFieldValues), schema))
    }

    val inputField = Literal.create(2, IntegerType)
    val expectedValue = create_row(2, 2)
    val expectedSchema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("a", IntegerType, nullable = false)))

    checkEvaluationCustom(
      AddFields(nonNullStruct :: Literal("a") :: inputField :: Nil),
      expectedValue,
      expectedSchema)

    checkEvaluationCustom(
      AddFields(unsafeRowStruct :: Literal("a") :: inputField :: Nil),
      expectedValue,
      expectedSchema)
  }
}

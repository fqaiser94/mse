package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class RenameFieldsTest extends ExpressionTester {

  val (nonNullStruct, nullStruct, unsafeRowStruct) = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", BooleanType),
      StructField("c", StringType)
    ))

    val fieldTypes = schema.fields.map(_.dataType)
    val fieldValues = Array(1, "hello", true, "world")
    val unsafeFieldValues = Array(1, UTF8String.fromString("hello"), true,
      UTF8String.fromString("world"))

    Tuple3(
      Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema),
      Literal.create(create_unsafe_row(fieldTypes, unsafeFieldValues), schema))
  }

  test("prettyName should return \"rename_fields\"") {
    assert(RenameFields(nullStruct, "a", "z").prettyName == "rename_fields")
  }

  test("checkInputDataTypes should fail if struct is not a struct dataType") {
    nonNullInputs.foreach {
      case input if input.dataType.typeName == "struct" =>
        val result = RenameFields(input, "a", "z").checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckSuccess
        assert(result == expected)
      case input =>
        val result = RenameFields(input, "a", "z").checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckFailure(
          s"struct should be struct data type. struct is ${input.expr.dataType.typeName}")
        assert(result == expected)
    }
  }

  test("checkInputDataTypes should succeed even if existingFieldName doesn't exist") {
    assert(RenameFields(nonNullStruct, "d", "z").checkInputDataTypes() ==
      TypeCheckResult.TypeCheckSuccess)
  }

  test("checkInputDataTypes should fail if existingFieldName passed in is null") {
    assert(RenameFields(nonNullStruct, null, "z").checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        "existingFieldName cannot be null"))
  }

  test("checkInputDataTypes should fail if newFieldName passed in is null") {
    assert(RenameFields(nonNullStruct, "a", null).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        "newFieldName cannot be null"))
  }

  test("checkInputDataTypes should fail if existingFieldNames is not the same length as newFieldNames") {
    assert(RenameFields(nonNullStruct, Seq("a"), Seq.empty).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        s"existingFieldNames should contain same number of elements as newFieldNames. existingFieldNames is of length 1. newFieldNames is of length 0."))

    assert(RenameFields(nonNullStruct, Seq.empty, Seq("a")).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        s"existingFieldNames should contain same number of elements as newFieldNames. existingFieldNames is of length 0. newFieldNames is of length 1."))
  }

  test("should return null if struct = null") {
    checkEvaluation(
      RenameFields(nullStruct, "a", "z"),
      null,
      StructType(Seq(
        StructField("z", IntegerType),
        StructField("b", StringType),
        StructField("c", BooleanType),
        StructField("c", StringType)
      )))
  }

  Seq(
    ("InternalRow", nonNullStruct),
    ("UnsafeRow", unsafeRowStruct)
  ).foreach { case (structName, struct) =>
    test(s"should rename field in $structName") {
      checkEvaluation(
        RenameFields(struct, "a", "z"),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("z", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should rename all fields with existingFieldName to newFieldName in $structName") {
      checkEvaluation(
        RenameFields(struct, "c", "z"),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("z", BooleanType),
          StructField("z", StringType)
        )))
    }

    test(s"should rename multiple fields in $structName") {
      checkEvaluation(
        RenameFields(struct, Seq("a", "b"), Seq("x", "y")),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("x", IntegerType),
          StructField("y", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should rename only the fields that exist in $structName when given multiple fields to rename") {
      checkEvaluation(
        RenameFields(struct, Seq("a", "z", "b"), Seq("x", "hello", "y")),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("x", IntegerType),
          StructField("y", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should rename existingFieldName to the first newFieldName in $structName, if existingFieldName appears multiple times in existingFieldNames") {
      checkEvaluation(
        RenameFields(struct, Seq("a", "a"), Seq("x", "y")),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("x", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }


    test(s"should return original struct if given fieldName does not exist in $structName") {
      checkEvaluation(
        RenameFields(struct, "d", "z"),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should work in a nested fashion on $structName") {
      checkEvaluation(
        RenameFields(RenameFields(struct, "a", "z"), "b", "y"),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("z", IntegerType),
          StructField("y", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }
  }
}

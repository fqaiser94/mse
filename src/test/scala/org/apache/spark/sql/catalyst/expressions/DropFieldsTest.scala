package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DropFieldsTest extends ExpressionTester {

  // TODO: test for handling attribute reference

  val (nonNullStruct, nullStruct, unsafeRowStruct) = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", BooleanType),
      StructField("c", StringType)))

    val fieldTypes = schema.fields.map(_.dataType)
    val fieldValues = Array(1, "hello", true, "world")
    val unsafeFieldValues = Array(1, UTF8String.fromString("hello"), true,
      UTF8String.fromString("world"))

    Tuple3(
      Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema),
      Literal.create(create_unsafe_row(fieldTypes, unsafeFieldValues), schema))
  }

  test("prettyName should return \"drop_field\"") {
    assert(DropFields(nullStruct, "a").prettyName == "drop_fields")
  }

  test("checkInputDataTypes should fail if struct is not a struct dataType") {
    nonNullInputs.foreach {
      case input if input.dataType.typeName == "struct" =>
        val result = DropFields(input, "a").checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckSuccess
        assert(result == expected)
      case input =>
        val result = DropFields(input, "a").checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckFailure(
          s"struct should be struct data type. struct is ${input.expr.dataType.typeName}")
        assert(result == expected)
    }
  }

  test("checkInputDataTypes should succeed even if fieldName doesn't exist") {
    assert(DropFields(nonNullStruct, "d").checkInputDataTypes() ==
      TypeCheckResult.TypeCheckSuccess)
  }

  test("checkInputDataTypes should succeed even if any of the given fieldNames don't exist") {
    assert(DropFields(nonNullStruct, "a", "d").checkInputDataTypes() ==
      TypeCheckResult.TypeCheckSuccess)
  }

  test("checkInputDataTypes should fail if any of the fieldNames passed in is null") {
    assert(DropFields(nonNullStruct, null).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        "fieldNames cannot contain null"))

    assert(DropFields(nonNullStruct, "a", null).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        "fieldNames cannot contain null"))
  }

  test("should return null if struct = null") {
    checkEvaluationCustom(
      DropFields(nullStruct, "a"),
      null,
      StructType(Seq(
        StructField("b", StringType),
        StructField("c", BooleanType),
        StructField("c", StringType))))
  }

  Seq(
    ("InternalRow", nonNullStruct),
    ("UnsafeRow", unsafeRowStruct)
  ).foreach { case (structName, struct) =>
    test(s"should drop field with given fieldName in $structName") {
      checkEvaluationCustom(
        DropFields(struct, "a"),
        create_row("hello", true, "world"),
        StructType(Seq(
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType))))

      checkEvaluationCustom(
        DropFields(struct, "b"),
        create_row(1, true, "world"),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("c", BooleanType),
          StructField("c", StringType))))
    }

    test(s"should drop all fields with given fieldName in $structName") {
      checkEvaluationCustom(
        DropFields(struct, "c"),
        create_row(1, "hello"),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))))
    }

    test(s"should drop all fields with given fieldNames in $structName") {
      checkEvaluationCustom(
        DropFields(struct, "a", "b"),
        create_row(true, "world"),
        StructType(Seq(
          StructField("c", BooleanType),
          StructField("c", StringType))))
    }

    test(s"should return empty struct if all fields in $structName are dropped") {
      checkEvaluationCustom(
        DropFields(struct, "a", "b", "c"),
        create_row(),
        StructType(Seq.empty))
    }

    test(s"should return original struct if given fieldName does not exist in $structName") {
      checkEvaluationCustom(
        DropFields(struct, "d"),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType))))
    }

    test(s"should return original struct if given fieldNames do not exist in $structName") {
      checkEvaluationCustom(
        DropFields(struct, "d", "e"),
        create_row(1, "hello", true, "world"),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType))))
    }

    test(s"should work in a nested fashion on $structName") {
      checkEvaluationCustom(
        DropFields(DropFields(struct, "a"), "b"),
        create_row(true, "world"),
        StructType(Seq(
          StructField("c", BooleanType),
          StructField("c", StringType))))
    }
  }
}

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.Matchers

class RenameFieldsTest extends ExpressionTester with Matchers {

  val (nonNullStruct, nullStruct, unsafeRowStruct, expectedValue) = {
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

    Tuple4(
      Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema),
      Literal.create(create_unsafe_row(fieldTypes, unsafeFieldValues), schema),
      create_row(fieldValues: _*))
  }

  test("prettyName should return \"rename_fields\"") {
    assert(RenameFields(nullStruct, "a", "z").prettyName == "rename_fields")
  }

  test("checkInputDataTypes should fail if struct is not a StructType dataType") {
    nonNullInputs.foreach {
      case input if input.dataType.typeName == "struct" =>
        val result = RenameFields(input :: Literal("a") :: Literal("z") :: Nil).checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckSuccess
        assert(result == expected)
      case input =>
        val result = RenameFields(input :: Literal("a") :: Literal("z") :: Nil).checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckFailure(
          s"Only struct is allowed to appear at first position, got: ${input.expr.dataType.typeName}.")
        assert(result == expected)
    }
  }

  test("checkInputDataTypes should fail if given existingFieldName is not a StringType dataType") {
    nonNullInputs.foreach {
      case input if input.dataType.typeName == StringType.typeName =>
        val result = RenameFields(nonNullStruct :: input :: Literal("z") :: Nil).checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckSuccess
        assert(result == expected)
      case input =>
        val result = RenameFields(nonNullStruct :: input :: Literal("z") :: Nil).checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckFailure(
          s"Only non-null foldable string expressions are allowed to appear after first position.")
        assert(result == expected)
    }
  }

  test("checkInputDataTypes should fail if given newFieldName is not a StringType dataType") {
    nonNullInputs.foreach {
      case input if input.dataType.typeName == StringType.typeName =>
        val result = RenameFields(nonNullStruct :: Literal("a") :: input :: Nil).checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckSuccess
        assert(result == expected)
      case input =>
        val result = RenameFields(nonNullStruct :: Literal("a") :: input :: Nil).checkInputDataTypes()
        val expected = TypeCheckResult.TypeCheckFailure(
          s"Only non-null foldable string expressions are allowed to appear after first position.")
        assert(result == expected)
    }
  }

  test("checkInputDataTypes should fail if existingFieldName passed in is null") {
    assert(
      RenameFields(nonNullStruct :: Literal.create(null, StringType) :: Literal("z") :: Nil).checkInputDataTypes() ==
        TypeCheckResult.TypeCheckFailure(
          s"Only non-null foldable string expressions are allowed to appear after first position."))

    assert(
      RenameFields(nonNullStruct :: null :: Literal("z") :: Nil).checkInputDataTypes() ==
        TypeCheckResult.TypeCheckFailure(
          s"Only non-null foldable string expressions are allowed to appear after first position."))
  }

  test("checkInputDataTypes should fail if newFieldName passed in is null") {
    assert(
      RenameFields(nonNullStruct :: Literal("a") :: Literal.create(null, StringType) :: Nil).checkInputDataTypes() ==
        TypeCheckResult.TypeCheckFailure(
          s"Only non-null foldable string expressions are allowed to appear after first position."))

    assert(
      RenameFields(nonNullStruct :: Literal("a") :: null :: Nil).checkInputDataTypes() ==
        TypeCheckResult.TypeCheckFailure(
          s"Only non-null foldable string expressions are allowed to appear after first position."))
  }

  test("checkInputDataTypes should succeed even if existingFieldName doesn't exist") {
    assert(RenameFields(nonNullStruct :: Literal("d") :: Literal("z") :: Nil).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckSuccess)
  }

  test("checkInputDataTypes should fail if an existingFieldName and newFieldName pair are not provided") {
    assert(RenameFields(nonNullStruct :: Literal("a") :: Nil).checkInputDataTypes() ==
      TypeCheckResult.TypeCheckFailure(
        s"rename_fields expects an odd number of arguments."))
  }

  test("should return null if struct = null") {
    checkEvaluationCustom(
      RenameFields(nullStruct :: Literal("a") :: Literal("z") :: Nil),
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
    test(s"should return original $structName") {
      checkEvaluationCustom(
        RenameFields(struct :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should rename field in $structName") {
      checkEvaluationCustom(
        RenameFields(struct :: Literal("a") :: Literal("z") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("z", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should rename all fields with existingFieldName to newFieldName in $structName") {
      checkEvaluationCustom(
        RenameFields(struct :: Literal("c") :: Literal("z") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("z", BooleanType),
          StructField("z", StringType)
        )))
    }

    test(s"should rename multiple fields in $structName") {
      checkEvaluationCustom(
        RenameFields(struct :: Literal("a") :: Literal("x") :: Literal("b") :: Literal("y") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("x", IntegerType),
          StructField("y", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should rename only the fields that exist in $structName when given multiple fields to rename") {
      checkEvaluationCustom(
        RenameFields(struct :: Literal("a") :: Literal("x") :: Literal("z") :: Literal("hello") :: Literal("b") :: Literal("y") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("x", IntegerType),
          StructField("y", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        ))
      )
    }

    test(s"should rename existingFieldName to newFieldName in $structName in the given order") {
      checkEvaluationCustom(
        // a is renamed to x
        // x is then renamed to y
        RenameFields(struct :: Literal("a") :: Literal("x") :: Literal("x") :: Literal("y") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("y", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))

      checkEvaluationCustom(
        // a is renamed to x
        // a no longer exists when user asks to rename a to y
        RenameFields(struct :: Literal("a") :: Literal("x") :: Literal("a") :: Literal("y") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("x", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }


    test(s"should return original struct if given fieldName does not exist in $structName") {
      checkEvaluationCustom(
        RenameFields(struct :: Literal("d") :: Literal("z") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }

    test(s"should work in a nested fashion on $structName") {
      checkEvaluationCustom(
        RenameFields(RenameFields(struct :: Literal("a") :: Literal("z") :: Nil) :: Literal("b") :: Literal("y") :: Nil),
        expectedValue,
        StructType(Seq(
          StructField("z", IntegerType),
          StructField("y", StringType),
          StructField("c", BooleanType),
          StructField("c", StringType)
        )))
    }
  }
}

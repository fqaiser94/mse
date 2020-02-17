package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AddField, Alias, CreateNamedStruct, DropFields, Expression, ExpressionEvalHelper, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

class CollapseSuccessiveAddFieldDropFieldsExpressionsTest extends PlanTest with ExpressionEvalHelper {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      CollapseSuccessiveAddFieldDropFieldsExpressions) :: Nil
  }

  protected def assertEquivalentPlanAndEvaluation(unoptimizedExpression: Expression, expectedExpression: Expression, expectedValue: Any, expectedDataType: DataType): Unit = {
    val actualPlan = Optimize.execute(Project(Alias(unoptimizedExpression, "out")() :: Nil, OneRowRelation()).analyze)
    val expectedPlan = Project(Alias(expectedExpression, "out")() :: Nil, OneRowRelation()).analyze

    comparePlans(actualPlan, expectedPlan)
    checkEvaluation(unoptimizedExpression, expectedValue)
    checkEvaluation(expectedExpression, expectedValue)
    assert(unoptimizedExpression.dataType == expectedDataType)
    assert(expectedExpression.dataType == expectedDataType)
  }

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false),
      StructField("c", IntegerType, nullable = false)))
    val fieldValues = Array(1, 2, 3)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should correctly combine AddField and DropFields into CreateNamedStruct, where AddField is being used to add a new field") {
    val newFieldValue = Literal.create(4, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", GetStructField(inputStruct, 0), "b", GetStructField(inputStruct, 1), "d", newFieldValue))
    val expectedEvaluationResult = create_row(1, 2, 4)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false),
      StructField("d", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddField(DropFields(inputStruct, "c"), "d", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)

    assertEquivalentPlanAndEvaluation(
      DropFields(AddField(inputStruct, "d", newFieldValue), "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine AddField and DropFields into CreateNamedStruct, where AddField is being used to replace an existing field") {
    val newFieldValue = Literal.create(4, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", newFieldValue, "b", GetStructField(inputStruct, 1)))
    val expectedEvaluationResult = create_row(4, 2)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddField(DropFields(inputStruct, "c"), "a", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)

    assertEquivalentPlanAndEvaluation(
      DropFields(AddField(inputStruct, "a", newFieldValue), "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine AddField and DropFields into CreateNamedStruct, where DropFields is being used to drop multiple fields") {
    val newFieldValue = Literal.create(4, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", GetStructField(inputStruct, 0), "d", newFieldValue))
    val expectedEvaluationResult = create_row(1, 4)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("d", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddField(DropFields(inputStruct, "b", "c"), "d", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)

    assertEquivalentPlanAndEvaluation(
      DropFields(AddField(inputStruct, "d", newFieldValue), "b", "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }
}

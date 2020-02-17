package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AddFields, Alias, CreateNamedStruct, Expression, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

class SimplifySuccessiveAddFieldsCreateNamedStructExpressionsTest extends PlanTest with ExpressionEvalHelper {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifySuccessiveAddFieldCreateNamedStructExpressions) :: Nil
  }

  // TODO: this function is copied in a bunch of places
  protected def assertEquivalentPlanAndEvaluation(unoptimizedExpression: Expression, expectedExpression: Expression, expectedValue: Any, expectedDataType: DataType): Unit = {
    val actualPlan = Optimize.execute(Project(Alias(unoptimizedExpression, "out")() :: Nil, OneRowRelation()).analyze)
    val expectedPlan = Project(Alias(expectedExpression, "out")() :: Nil, OneRowRelation()).analyze

    comparePlans(actualPlan, expectedPlan)
    checkEvaluation(unoptimizedExpression, expectedValue)
    checkEvaluation(expectedExpression, expectedValue)
    assert(unoptimizedExpression.dataType == expectedDataType)
    assert(expectedExpression.dataType == expectedDataType)
  }

  test("should correctly combine AddField and CreateNamedStruct into CreateNamedStruct, where AddField is being used to add a new field") {
    val newFieldValue = Literal.create(2, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", newFieldValue))
    val expectedEvaluationResult = create_row(1, 2)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType))), "b", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine AddField and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and AddField is being used to add a new field") {
    val newFieldValue = Literal.create(3, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType), "c", newFieldValue))
    val expectedEvaluationResult = create_row(1, 2, 3)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false),
      StructField("c", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), "c", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine AddField and CreateNamedStruct into CreateNamedStruct, where AddField is being used to replace a an existing field") {
    val newFieldValue = Literal.create(2, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", newFieldValue))
    val expectedEvaluationResult = create_row(2)
    val expectedDataType = StructType(Seq(StructField("a", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType))), "a", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine AddField and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and AddField is being used to replace a an existing field") {
    val newFieldValue = Literal.create(3, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", newFieldValue))
    val expectedEvaluationResult = create_row(1, 3)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), "b", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }
}

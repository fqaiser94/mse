package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, DropFields, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifySuccessiveDropFieldsCreateNamedStructExpressionsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifySuccessiveDropFieldsCreateNamedStructExpressions) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  test("should correctly combine DropFields and CreateNamedStruct into CreateNamedStruct, where DropFields is being used to drop a single field") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType)))
    val expectedEvaluationResult = create_row(1)
    val expectedDataType = StructType(Seq(StructField("a", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), "b"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and DropFields is being used to drop multiple fields") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType)))
    val expectedEvaluationResult = create_row(1)
    val expectedDataType = StructType(Seq(StructField("a", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType), "c", Literal.create(3, IntegerType))), "b", "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields and CreateNamedStruct into CreateNamedStruct, where DropFields is being used to drop the only existing field") {
    val expectedExpression = CreateNamedStruct(Seq.empty)
    val expectedEvaluationResult = create_row()
    val expectedDataType = StructType(Seq.empty)

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType))), "a"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields and CreateNamedStruct into CreateNamedStruct, where DropFields is being used to drop all existing fields") {
    val expectedExpression = CreateNamedStruct(Seq.empty)
    val expectedEvaluationResult = create_row()
    val expectedDataType = StructType(Seq.empty)

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), "a", "b"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields and CreateNamedStruct into CreateNamedStruct, where DropFields is being used to drop a non-existent field") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType)))
    val expectedEvaluationResult = create_row(1)
    val expectedDataType = StructType(Seq(StructField("a", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType))), "b"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType))), "b", "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields and CreateNamedStruct into CreateNamedStruct, where DropFields is being used to drop a mixture of existent and non-existent fields") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType)))
    val expectedEvaluationResult = create_row(1)
    val expectedDataType = StructType(Seq(StructField("a", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      DropFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType), "c", Literal.create(3, IntegerType))), "b", "d", "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  // TODO: do I need a test for null struct?
}
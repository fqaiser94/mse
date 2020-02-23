package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, DropFields, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifyStructExpressionsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch("SimplifyStructExpressions", FixedPoint(50), SimplifyStructExpressions.rules: _*) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  test("should correctly combine AddFields, DropFields, and CreateNamedStruct into CreateNamedStruct") {
    val expectedExpression = CreateNamedStruct(Seq(Literal("a"), Literal(1, IntegerType), Literal("b"), Literal(3, IntegerType)))
    val expectedEvaluationResult = create_row(1, 3)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddFields(DropFields(CreateNamedStruct(Seq(Literal("a"), Literal(1, IntegerType), Literal("b"), Literal(2, IntegerType))), "b"), "b", Literal(3, IntegerType)),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields, AddFields, and CreateNamedStruct into CreateNamedStruct #1") {
    val expectedExpression = CreateNamedStruct(Seq(Literal("a"), Literal(1, IntegerType)))
    val expectedEvaluationResult = create_row(1)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      DropFields(AddFields(CreateNamedStruct(Seq(Literal("a"), Literal(1, IntegerType), Literal("b"), Literal(2, IntegerType))), "b", Literal(3, IntegerType)), "b"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine DropFields, AddFields, and CreateNamedStruct into CreateNamedStruct #2") {
    val expectedExpression = CreateNamedStruct(Seq(Literal("a"), Literal(1, IntegerType), Literal("c"), Literal(4, IntegerType)))
    val expectedEvaluationResult = create_row(1, 4)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("c", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      DropFields(AddFields(CreateNamedStruct(Seq(Literal("a"), Literal(1, IntegerType), Literal("b"), Literal(2, IntegerType))), Seq("b", "c"), Seq(Literal(3, IntegerType), Literal(4, IntegerType))), "b", "e"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }
}
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifyRenameFieldsCreateNamedStructTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifyRenameFieldsCreateNamedStruct) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  test("should correctly combine RenameFields and CreateNamedStruct into CreateNamedStruct, where RenameFields is being used to rename a single field") {
    val expectedExpression = CreateNamedStruct(Seq("b", Literal.create(1, IntegerType)))
    val expectedEvaluationResult = create_row(1)
    val expectedDataType = StructType(Seq(StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType))), "a", "b"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine RenameFields and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and RenameFields is being used to rename a single field") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "c", Literal.create(2, IntegerType)))
    val expectedEvaluationResult = create_row(1, 2)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("c", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), "b", "c"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine RenameFields and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and RenameFields is being used to rename multiple fields with the same name") {
    val expectedExpression = CreateNamedStruct(Seq("b", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType)))
    val expectedEvaluationResult = create_row(1, 2)
    val expectedDataType = StructType(Seq(
      StructField("b", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "a", Literal.create(2, IntegerType))), "a", "b"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine RenameFields and CreateNamedStruct into CreateNamedStruct, where RenameFields is being used to rename multiple fields") {
    val expectedExpression = CreateNamedStruct(Seq("c", Literal.create(1, IntegerType), "d", Literal.create(2, IntegerType)))
    val expectedEvaluationResult = create_row(1, 2)
    val expectedDataType = StructType(Seq(
      StructField("c", IntegerType, nullable = false),
      StructField("d", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), Seq("a", "b"), Seq("c", "d")),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine RenameFields and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and RenameFields is being used to rename non-existent field/s") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType)))
    val expectedEvaluationResult = create_row(1, 2)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), "c", "x"),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType))), Seq("c", "d"), Seq("x", "y")),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  test("should correctly combine RenameFields and CreateNamedStruct into CreateNamedStruct, where original-CreateNamedStruct has multiple children and RenameFields is being used to rename a mixture of existent and non-existent fields") {
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "x", Literal.create(2, IntegerType), "y", Literal.create(3, IntegerType)))
    val expectedEvaluationResult = create_row(1, 2, 3)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("x", IntegerType, nullable = false),
      StructField("y", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      RenameFields(CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType), "c", Literal.create(3, IntegerType))), Seq("b", "d", "c"), Seq("x", "z", "y")),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)
  }

  // TODO: do I need a null struct test?
}
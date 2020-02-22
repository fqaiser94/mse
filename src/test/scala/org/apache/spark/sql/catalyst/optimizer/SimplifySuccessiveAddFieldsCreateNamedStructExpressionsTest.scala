package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifySuccessiveAddFieldsCreateNamedStructExpressionsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifySuccessiveAddFieldsCreateNamedStructExpressions) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

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

  // TODO: test for null struct
}

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifySuccessiveRenameFieldsExpressionsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifySuccessiveRenameFieldsExpressions) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))
    val fieldValues = Array(1, 2, 3)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should simplify successive RenameFields call into a single RenameFields call") {
    val expectedEvaluationResult = create_row(1, 2, 3)

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(inputStruct, "a", "x"), "b", "y"),
      RenameFields(inputStruct, Seq("a", "b"), Seq("x", "y")),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("c", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(inputStruct, Seq("a", "b"), Seq("x", "y")), "c", "z"),
      RenameFields(inputStruct, Seq("a", "b", "c"), Seq("x", "y", "z")),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(inputStruct, "a", "x"), Seq("b", "c"), Seq("y", "z")),
      RenameFields(inputStruct, Seq("a", "b", "c"), Seq("x", "y", "z")),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))
  }

  // TODO: test for null struct
}

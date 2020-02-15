package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, DropFields, Expression, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CollapseSuccessiveDropFieldsExpressionsTest extends PlanTest with ExpressionEvalHelper {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      "CollapseDropFieldsExpressionsTest",
      FixedPoint(50),
      SimplifyStructManipulationExpressions) :: Nil
  }

  protected def assertEquivalentPlanAndEvaluation(e1: Expression, e2: Expression, value: Any): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)
    comparePlans(actual, correctAnswer)
    checkEvaluation(e1, value)
    checkEvaluation(e2, value)
  }

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType),
      StructField("d", IntegerType),
      StructField("e", IntegerType),
      StructField("f", IntegerType)))
    val fieldValues = Array(1, 2, 3, 4, 5, 6)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should collapse successive DropFields call into a single DropFields call") {
    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "b"), "c"),
      DropFields(inputStruct, "b", "c"),
      create_row(1, 4, 5, 6))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "a", "b"), "c"),
      DropFields(inputStruct, "a", "b", "c"),
      create_row(4, 5, 6))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "a"), "b", "c"),
      DropFields(inputStruct, "a", "b", "c"),
      create_row(4, 5, 6))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "a", "b"), "c", "d"),
      DropFields(inputStruct, "a", "b", "c", "d"),
      create_row(5, 6))
  }
}

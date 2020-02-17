package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExpressionEvalHelper, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

class SimplifySuccessiveRenameFieldsExpressionsTest extends PlanTest with ExpressionEvalHelper {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifySuccessiveRenameFieldsExpressions) :: Nil
  }

  /**
    * Checks both expressions resolve to an equivalent plan, evaluation, and dataType.
    */
  protected def assertEquivalent(unoptimizedExpression: Expression, expectedExpression: Expression, expectedValue: Any, expectedDataType: DataType): Unit = {
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
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))
    val fieldValues = Array(1, 2, 3)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should simplify successive RenameFields call into a single RenameFields call") {
    val expectedEvaluationResult = create_row(1, 2, 3)

    assertEquivalent(
      RenameFields(RenameFields(inputStruct, "a", "x"), "b", "y"),
      RenameFields(inputStruct, Seq("a", "b"), Seq("x", "y")),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("c", IntegerType))))

    assertEquivalent(
      RenameFields(RenameFields(inputStruct, Seq("a", "b"), Seq("x", "y")), "c", "z"),
      RenameFields(inputStruct, Seq("a", "b", "c"), Seq("x", "y", "z")),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))

    assertEquivalent(
      RenameFields(RenameFields(inputStruct, "a", "x"), Seq("b", "c"), Seq("y", "z")),
      RenameFields(inputStruct, Seq("a", "b", "c"), Seq("x", "y", "z")),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))
  }
}

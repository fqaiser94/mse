package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AddField, Alias, CreateNamedStruct, DropFields, Expression, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

class SimplifyStructManipulationExpressionsTest extends PlanTest with ExpressionEvalHelper {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      "CollapseDropFieldsExpressionsTest",
      FixedPoint(50),
      SimplifyStructManipulationExpressions) :: Nil
  }

  protected def assertEquivalentPlanAndEvaluation(e1: Expression, e2: Expression, expectedValue: Any, expectedDataType: DataType): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)

    // TODO: delete
    println(actual.treeString)

    //    comparePlans(actual, correctAnswer)
    checkEvaluation(e1, expectedValue)
    checkEvaluation(e2, expectedValue)
    assert(e1.dataType == expectedDataType)
    assert(e2.dataType == expectedDataType)
  }

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))
    val fieldValues = Array(1, 2, 3)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should combine AddField and DropFields call into a single CreateNamedStruct call") {
    val newFieldValue = Literal.create(4, IntegerType)
    val expectedExpression = CreateNamedStruct(Seq("a", Literal.create(1, IntegerType), "b", Literal.create(2, IntegerType), "d", newFieldValue))
    val expectedEvaluationResult = create_row(1, 2, 4)
    val expectedDataType = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("d", IntegerType, nullable = false)))

    assertEquivalentPlanAndEvaluation(
      AddField(DropFields(inputStruct, "c"), "d", newFieldValue),
      expectedExpression,
      expectedEvaluationResult,
      expectedDataType)

    //    assertEquivalentPlanAndEvaluation(
    //      DropFields(AddField(inputStruct, "d", newFieldValue), "c"),
    //      expectedExpression,
    //      expectedEvaluationResult,
    //      expectedDataType)
  }
}

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExpressionEvalHelper}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DataType

trait OptimizerTest extends PlanTest with ExpressionEvalHelper {

  val Optimizer: RuleExecutor[LogicalPlan]

  protected def assertEquivalentPlanAndEvaluation(unoptimizedExpression: Expression, expectedExpression: Expression, expectedValue: Any, expectedDataType: DataType): Unit = {
    val actualPlan = Optimizer.execute(Project(Alias(unoptimizedExpression, "out")() :: Nil, OneRowRelation()).analyze)
    val expectedPlan = Project(Alias(expectedExpression, "out")() :: Nil, OneRowRelation()).analyze

    comparePlans(actualPlan, expectedPlan)
    checkEvaluation(unoptimizedExpression, expectedValue)
    checkEvaluation(expectedExpression, expectedValue)
    assert(unoptimizedExpression.dataType == expectedDataType)
    assert(expectedExpression.dataType == expectedDataType)
  }

}

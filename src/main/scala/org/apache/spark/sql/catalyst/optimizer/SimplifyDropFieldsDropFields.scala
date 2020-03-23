package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.DropFields
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyDropFieldsDropFields extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case DropFields(DropFields(exprsA) :: exprsB) =>
      DropFields(exprsA ++ exprsB)
  }
}

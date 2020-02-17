package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.RenameFields
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifySuccessiveRenameFieldsExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case RenameFields(RenameFields(struct, existingA, newA), existingB, newB) =>
      RenameFields(struct, existingA ++ existingB, newA ++ newB)
  }
}
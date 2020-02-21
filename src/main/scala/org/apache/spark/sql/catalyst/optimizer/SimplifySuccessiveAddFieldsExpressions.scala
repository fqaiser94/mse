package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.AddFields
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifySuccessiveAddFieldsExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddFields(AddFields(struct, namesA, expressionsA), namesB, expressionsB) =>
      AddFields(struct, namesA ++ namesB, expressionsA ++ expressionsB)
  }
}

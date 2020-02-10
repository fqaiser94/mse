package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Simplifies struct expressions.
  */
object SimplifyStructExpressions extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case RenameField(RenameField(struct, og1, new1), og2, new2) =>
      RenameField(struct, og2, new2)
  }
}

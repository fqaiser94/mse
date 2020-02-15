package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Simplifies struct expressions.
  */
object SimplifyStructExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case RenameField(RenameField(struct, og1, new1), og2, new2) =>
      Literal("hello")
    case DropFields(DropFields(struct, a), b) =>
      Literal("hello")
    case AddField(AddField(struct, name1, value1), name2, value2) =>
      Literal("hello")
  }
}

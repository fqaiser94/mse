package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyStructBasedExpressions {
  val rules: Seq[Rule[LogicalPlan]] = Seq(
    // TODO: add add
    // TODO: rename rename
    // drop drop
    CollapseSuccessiveDropFieldsExpressions,
    // TODO: add rename
    // TODO: rename add
    // TODO: drop rename
    // TODO: rename drop
    // add drop
    // drop add
    CollapseSuccessiveAddFieldDropFieldsExpressions,
    // add struct
    CollapseSuccessiveCreateNamedStructAddFieldExpressions
    // TODO: rename struct
    // TODO: drop struct
  )
}
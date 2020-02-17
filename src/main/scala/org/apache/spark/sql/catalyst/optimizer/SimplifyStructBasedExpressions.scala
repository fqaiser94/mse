package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyStructBasedExpressions {
  val rules: Seq[Rule[LogicalPlan]] = Seq(
    // TODO: add add
    // TODO: rename rename
    // drop drop
    SimplifySuccessiveDropFieldsExpressions,
    // TODO: add rename
    // TODO: rename add
    // TODO: drop rename
    // TODO: rename drop
    // add drop
    // drop add
    SimplifySuccessiveAddFieldDropFieldsExpressions,
    // add struct
    SimplifySuccessiveAddFieldCreateNamedStructExpressions
    // TODO: rename struct
    // TODO: drop struct
  )
}
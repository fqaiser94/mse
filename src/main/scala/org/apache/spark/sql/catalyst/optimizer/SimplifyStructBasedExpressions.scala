package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyStructBasedExpressions {
  val rules: Seq[Rule[LogicalPlan]] = Seq(
    // add add
    SimplifySuccessiveAddFieldsExpressions,
    // rename rename
    SimplifySuccessiveRenameFieldsExpressions,
    // drop drop
    SimplifySuccessiveDropFieldsExpressions,
    // TODO: add rename
    // TODO: rename add
    // drop rename
    // rename drop
    SimplifySuccessiveRenameFieldsDropFieldsExpressions,
    // add drop
    // drop add
    SimplifySuccessiveAddFieldsDropFieldsExpressions,
    // add struct
    SimplifySuccessiveAddFieldsCreateNamedStructExpressions
    // TODO: rename struct
    // TODO: drop struct
  )
}
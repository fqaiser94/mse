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
    // add rename
    // rename add
    SimplifySuccessiveAddFieldsRenameFieldsExpressions,
    // drop rename
    // rename drop
    SimplifySuccessiveRenameFieldsDropFieldsExpressions,
    // add drop
    // drop add
    SimplifySuccessiveAddFieldsDropFieldsExpressions,
    // add struct
    SimplifySuccessiveAddFieldsCreateNamedStructExpressions,
    // rename struct
    SimplifySuccessiveRenameFieldsCreateNamedStructExpressions
    // TODO: drop struct
  )
}
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyStructExpressions {
  val rules: Seq[Rule[LogicalPlan]] = Seq(
    // add add
    SimplifyAddFieldsAddFields,
    // rename rename
    SimplifyRenameFieldsRenameFields,
    // drop drop
    SimplifyDropFieldsDropFields
  )
}
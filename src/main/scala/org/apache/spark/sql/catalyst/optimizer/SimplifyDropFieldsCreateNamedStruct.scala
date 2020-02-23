package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, DropFields, EmptyRow, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyDropFieldsCreateNamedStruct extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case DropFields(struct@CreateNamedStruct(_), dropFieldNames@_*) =>
      val existingFields: Seq[(String, Expression)] = struct.nameExprs.zip(struct.valExprs).map { case (nameExpr, valExpr) =>
        val name = nameExpr.eval(EmptyRow).toString
        (name, valExpr)
      }
      val newFields: Seq[(String, Expression)] = existingFields.filter { case (name, expr) => !dropFieldNames.contains(name) }
      CreateNamedStruct(newFields.flatMap { case (name, expr) => Seq(Literal(name), expr) })
  }
}
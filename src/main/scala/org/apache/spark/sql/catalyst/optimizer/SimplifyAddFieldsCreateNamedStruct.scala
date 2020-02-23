package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, EmptyRow, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.Utilities._

object SimplifyAddFieldsCreateNamedStruct extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddFields(struct@CreateNamedStruct(_), addOrReplaceFieldNames, addOrReplaceFieldExprs) =>
      val existingFields: Seq[(String, Expression)] = struct.nameExprs.zip(struct.valExprs).map { case (nameExpr, valExpr) =>
        val name = nameExpr.eval(EmptyRow).toString
        (name, valExpr)
      }
      val addOrReplaceFields: Seq[(String, Expression)] = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)
      val newFields: Seq[Expression] = loop(existingFields, addOrReplaceFields).flatMap { case (name, expr) => Seq(Literal(name), expr) }

      CreateNamedStruct(newFields)
  }
}

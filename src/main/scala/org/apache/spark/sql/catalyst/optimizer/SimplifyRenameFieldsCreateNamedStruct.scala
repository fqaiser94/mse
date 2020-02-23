package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, EmptyRow, Expression, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifyRenameFieldsCreateNamedStruct extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case RenameFields(struct@CreateNamedStruct(_), existingFieldNames, newFieldNames) =>
      val existingFields: Seq[(String, Expression)] = struct.nameExprs.zip(struct.valExprs).map { case (nameExpr, valExpr) =>
        val name = nameExpr.eval(EmptyRow).toString
        (name, valExpr)
      }
      val renamedFields = {
        var fields = existingFields
        existingFieldNames.zip(newFieldNames).foreach { case (existingName, newName) =>
          fields = fields.map {
            case (fieldName, expr) if fieldName == existingName => (newName, expr)
            case x => x
          }
        }
        fields
      }

      CreateNamedStruct(renamedFields.flatMap { case (name, expr) => Seq(Literal(name), expr) })
  }
}
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.Utilities._
import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, EmptyRow, Expression, If, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

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
    // TODO: is this a good idea?
    case AddFields(If(IsNotNull(_), struct@CreateNamedStruct(_), Literal(null, _)), addOrReplaceFieldNames, addOrReplaceFieldExprs) =>
      val existingFields: Seq[(String, Expression)] = struct.nameExprs.zip(struct.valExprs).map { case (nameExpr, valExpr) =>
        val name = nameExpr.eval(EmptyRow).toString
        (name, valExpr)
      }
      val addOrReplaceFields: Seq[(String, Expression)] = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)
      val newFields: Seq[Expression] = loop(existingFields, addOrReplaceFields).flatMap { case (name, expr) => Seq(Literal(name), expr) }

      val temp = CreateNamedStruct(newFields)
      If(IsNotNull(struct), temp, Literal(null, temp.dataType))
  }
}

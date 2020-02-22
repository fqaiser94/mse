package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, EmptyRow, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifySuccessiveAddFieldCreateNamedStructExpressions extends Rule[LogicalPlan] {
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

  // TODO: function is used in a bunch of places
  private def loop[V](existingFields: Seq[(String, V)], addOrReplaceFields: Seq[(String, V)]): Seq[(String, V)] = {
    if (addOrReplaceFields.nonEmpty) {
      val existingFieldNames = existingFields.map(_._1)
      val newField@(newFieldName, _) = addOrReplaceFields.head

      if (existingFieldNames.contains(newFieldName)) {
        loop(
          existingFields.map {
            case (fieldName, _) if fieldName == newFieldName => newField
            case x => x
          },
          addOrReplaceFields.drop(1))
      } else {
        loop(
          existingFields :+ newField,
          addOrReplaceFields.drop(1))
      }
    } else {
      existingFields
    }
  }
}

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddField, CreateStruct, DropFields, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object SimplifyStructManipulationExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    // TODO: should be dropFields, plural, with test
    case AddField(DropFields(expr, dropField), newFieldName, newFieldExpr) =>
      val fields = {
        val fields = expr.dataType.asInstanceOf[StructType].fields
        val temp = fields.zipWithIndex
          .collect {
            case (field, i) if field.name != dropField => GetStructField(expr, i)
            case (field, _) if field.name == newFieldName => newFieldExpr
          }

        if (fields.exists(x => x.name == newFieldName)) {
          temp
        } else {
          temp :+ newFieldExpr
        }
      }

      CreateStruct(fields)
  }
}

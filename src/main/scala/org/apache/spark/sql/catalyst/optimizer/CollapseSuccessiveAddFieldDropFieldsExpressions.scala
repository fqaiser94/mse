package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddField, CreateNamedStruct, DropFields, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object CollapseSuccessiveAddFieldDropFieldsExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddField(DropFields(structExpr, dropFields@_*), newFieldName, newFieldExpr) =>
      toCreateNamedStruct(structExpr, dropFields, newFieldName, newFieldExpr)
    case DropFields(AddField(structExpr, newFieldName, newFieldExpr), dropFields@_*) =>
      toCreateNamedStruct(structExpr, dropFields, newFieldName, newFieldExpr)
  }


  private def toCreateNamedStruct(structExpr: Expression, dropFields: Seq[String], newFieldName: String, newFieldExpr: Expression): CreateNamedStruct = {
    val fields: Seq[Expression] = {
      val fields = structExpr.dataType.asInstanceOf[StructType].fields
      val temp = fields.zipWithIndex.flatMap {
        case (field, i) if !dropFields.contains(field.name) => Seq(Literal(field.name), GetStructField(structExpr, i))
        case (field, _) if field.name == newFieldName => Seq(Literal(newFieldName), newFieldExpr)
        case _ => Seq.empty
      }

      if (fields.exists(x => x.name == newFieldName)) {
        temp
      } else {
        temp ++ Seq(Literal(newFieldName), newFieldExpr)
      }
    }

    CreateNamedStruct(fields)
  }
}

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddField, CreateNamedStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object CollapseSuccessiveCreateNamedStructAddFieldExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddField(structExpr@CreateNamedStruct(_), newFieldName, newFieldExpr) =>
      toCreateNamedStruct(structExpr, Seq.empty, newFieldName, newFieldExpr)
  }

  // TODO: this function is copied in a bunch of places
  private def toCreateNamedStruct(structExpr: Expression, dropFields: Seq[String], newFieldName: String, newFieldExpr: Expression): CreateNamedStruct = {
    val fieldNames: Array[String] = structExpr.dataType.asInstanceOf[StructType].fieldNames.filter(fieldName => !dropFields.contains(fieldName))

    val newFieldIdx: Int = fieldNames.indexOf(newFieldName) match {
      case -1 => fieldNames.length
      case x => x
    }

    val fields: Seq[Expression] =
      fieldNames
        .patch(newFieldIdx, Seq(newFieldName), 1)
        .zipWithIndex
        .flatMap {
          case (fieldName, _) if fieldName == newFieldName => Seq(Literal(fieldName), newFieldExpr)
          case (fieldName, i) => Seq(Literal(fieldName), GetStructField(structExpr, i))
        }

    CreateNamedStruct(fields)
  }
}

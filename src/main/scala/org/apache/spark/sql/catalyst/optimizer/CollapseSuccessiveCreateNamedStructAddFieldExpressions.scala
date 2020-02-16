package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddField, CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object CollapseSuccessiveCreateNamedStructAddFieldExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddField(structExpr@CreateNamedStruct(_), newFieldName, newFieldExpr) =>
      toCreateNamedStruct(structExpr, newFieldName, newFieldExpr)
  }

  // TODO: this function is copied in a bunch of places
  private def toCreateNamedStruct(structExpr: Expression, newFieldName: String, newFieldExpr: Expression): CreateNamedStruct = {
    val fieldNames: Array[String] = structExpr.dataType.asInstanceOf[StructType].fieldNames

    val newFieldIdx: Int = fieldNames.indexOf(newFieldName) match {
      case -1 => fieldNames.length
      case x => x
    }

    val fields: Seq[Expression] =
      structExpr
        .children
        .grouped(2)
        .toSeq
        .patch(newFieldIdx, Seq(Seq(Literal(newFieldName), newFieldExpr)), 1)
        .flatten

    CreateNamedStruct(fields)
  }
}

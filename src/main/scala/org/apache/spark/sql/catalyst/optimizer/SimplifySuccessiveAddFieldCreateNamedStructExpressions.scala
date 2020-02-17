package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object SimplifySuccessiveAddFieldCreateNamedStructExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddFields(structExpr: Expression, newFieldNames, newFieldExprs) =>
      toCreateNamedStruct(structExpr, newFieldNames, newFieldExprs)
  }

  // TODO: this function is copied in a bunch of places
  private def toCreateNamedStruct(structExpr: Expression, newFieldNames: Seq[String], newFieldExpr: Seq[Expression]): CreateNamedStruct = {
    //    val fieldNames: Array[String] = structExpr.dataType.asInstanceOf[StructType].fieldNames
    //
    //    val newFieldIdx: Int = fieldNames.indexOf(newFieldName) match {
    //      case -1 => fieldNames.length
    //      case x => x
    //    }
    //
    //    val fields: Seq[Expression] =
    //      structExpr
    //        .children
    //        .grouped(2)
    //        .toSeq
    //        .patch(newFieldIdx, Seq(Seq(Literal(newFieldName), newFieldExpr)), 1)
    //        .flatten

    CreateNamedStruct(Seq.empty)
  }
}

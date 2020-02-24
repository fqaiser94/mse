package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.Utilities.loop
import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, GetStructField, If, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

// TODO: do I actually need this?
object ReplaceAddFields extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddFields(structExpr, addOrReplaceFieldNames, addOrReplaceFieldExprs) =>
      val existingFields = structExpr.dataType.asInstanceOf[StructType].fieldNames.zipWithIndex
        .map { case (fieldName, i) => (fieldName, GetStructField(structExpr, i)) }

      val addOrReplaceFields = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)

      val newFields = loop(existingFields, addOrReplaceFields)
        .flatMap { case (name, expr) => Seq(Literal(name), expr) }

      val temp = CreateNamedStruct(newFields)
      If(IsNotNull(structExpr), temp, Literal(null, temp.dataType))
  }
}
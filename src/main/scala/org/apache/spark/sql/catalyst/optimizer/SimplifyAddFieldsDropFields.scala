package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, DropFields, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.Utilities._

object SimplifyAddFieldsDropFields extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddFields(DropFields(structExpr, dropFields@_*), addOrReplaceFieldNames, addOrReplaceFieldExprs) =>
      val existingFields: Seq[(String, Expression)] = structExpr
        .dataType
        .asInstanceOf[StructType]
        .fieldNames
        .zipWithIndex
        .filter { case (fieldName, i) => !dropFields.contains(fieldName) }
        .map { case (fieldName, i) => (fieldName, GetStructField(structExpr, i)) }
      val addOrReplaceFields: Seq[(String, Expression)] = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)
      val newFields: Seq[Expression] = loop(existingFields, addOrReplaceFields).flatMap { case (name, expr) => Seq(Literal(name), expr) }

      CreateNamedStruct(newFields)
    case DropFields(AddFields(structExpr, addOrReplaceFieldNames, addOrReplaceFieldExprs), dropFields@_*) =>
      val existingFields: Seq[(String, Expression)] = structExpr
        .dataType
        .asInstanceOf[StructType]
        .fieldNames
        .zipWithIndex
        .map { case (fieldName, i) => (fieldName, GetStructField(structExpr, i)) }
      val addOrReplaceFields: Seq[(String, Expression)] = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)
      val newFields: Seq[Expression] = loop(existingFields, addOrReplaceFields)
        .filter { case (fieldName, _) => !dropFields.contains(fieldName) }
        .flatMap { case (name, expr) => Seq(Literal(name), expr) }

      CreateNamedStruct(newFields)
  }
}

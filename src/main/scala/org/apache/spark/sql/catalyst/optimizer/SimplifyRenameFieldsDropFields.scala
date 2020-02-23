package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, DropFields, Expression, GetStructField, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object SimplifyRenameFieldsDropFields extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case RenameFields(DropFields(structExpr, dropFields@_*), existingFieldNames, newFieldNames) =>
      toCreateNamedStruct(structExpr, existingFieldNames, newFieldNames, dropFields, Seq.empty)
    case DropFields(RenameFields(structExpr, existingFieldNames, newFieldNames), dropFields@_*) =>
      toCreateNamedStruct(structExpr, existingFieldNames, newFieldNames, Seq.empty, dropFields)
  }

  private def toCreateNamedStruct(structExpr: Expression, existingFieldNames: Seq[String], newFieldNames: Seq[String], dropFieldsBeforeRename: Seq[String], dropFieldsAfterRename: Seq[String]): CreateNamedStruct = {
    val fields: Seq[Expression] =
      structExpr
        .dataType
        .asInstanceOf[StructType]
        .fields
        .zipWithIndex
        .filter { case (field, _) => !dropFieldsBeforeRename.contains(field.name) }
        .map { case (field, i) =>
          val fieldName = existingFieldNames.zip(newFieldNames).collectFirst {
            case (existingFieldName, newFieldName) if existingFieldName == field.name => newFieldName
          }.getOrElse(field.name)

          (fieldName, GetStructField(structExpr, i))
        }
        .filter { case (fieldName, _) => !dropFieldsAfterRename.contains(fieldName) }
        .flatMap { case (fieldName, expression) => Seq(Literal(fieldName), expression) }

    CreateNamedStruct(fields)
  }
}

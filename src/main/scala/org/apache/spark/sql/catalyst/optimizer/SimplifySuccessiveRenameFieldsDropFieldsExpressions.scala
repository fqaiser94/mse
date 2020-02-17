package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, DropFields, Expression, GetStructField, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object SimplifySuccessiveRenameFieldsDropFieldsExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case RenameFields(DropFields(structExpr, dropFields@_*), existingFieldNames, newFieldNames) =>
      val pairs = existingFieldNames.zip(newFieldNames)
      val fields: Seq[Expression] =
        structExpr
          .dataType
          .asInstanceOf[StructType]
          .fields
          .zipWithIndex
          .filter { case (field, i) => !dropFields.contains(field.name) }
          .flatMap {
            case (field, i) =>
              val fieldName = pairs.collectFirst {
                case (existingFieldName, newFieldName) if existingFieldName == field.name => newFieldName
              }.getOrElse(field.name)

              Seq(Literal(fieldName), GetStructField(structExpr, i))
          }

      CreateNamedStruct(fields)
    case DropFields(RenameFields(structExpr, existingFieldNames, newFieldNames), dropFields@_*) =>
      val pairs = existingFieldNames.zip(newFieldNames)
      val fields: Seq[Expression] =
        structExpr
          .dataType
          .asInstanceOf[StructType]
          .fields
          .zipWithIndex
          .map {
            case (field, i) =>
              val fieldName = pairs.collectFirst {
                case (existingFieldName, newFieldName) if existingFieldName == field.name => newFieldName
              }.getOrElse(field.name)

              (fieldName, GetStructField(structExpr, i))
          }
          .filter { case (fieldName, _) => !dropFields.contains(fieldName) }
          .flatMap {
            case (fieldName, expression) => Seq(Literal(fieldName), expression)
          }

      CreateNamedStruct(fields)
  }
}

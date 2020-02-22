package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, Expression, GetStructField, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object SimplifySuccessiveAddFieldsRenameFieldsExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case AddFields(RenameFields(structExpr, existingFieldNames, newFieldNames), addOrReplaceFieldNames, addOrReplaceFieldExprs) =>
      val renamedFields: Seq[(String, Expression)] = {
        var fields = structExpr.dataType.asInstanceOf[StructType].fields.zipWithIndex.map { case (structField, i) =>
          (structField.name, GetStructField(structExpr, i))
        }

        existingFieldNames.zip(newFieldNames).foreach { case (existingName, newName) =>
          fields = fields.map {
            case (name, expr) if name == existingName => (newName, expr)
            case x => x
          }
        }
        fields
      }
      val addOrReplaceFields: Seq[(String, Expression)] = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)
      val newFields: Seq[Expression] = loop(renamedFields, addOrReplaceFields).flatMap { case (name, expr) => Seq(Literal(name), expr) }

      CreateNamedStruct(newFields)
    case RenameFields(AddFields(structExpr, addOrReplaceFieldNames, addOrReplaceFieldExprs), existingFieldNames, newFieldNames) =>
      val existingFields: Seq[(String, Expression)] = structExpr.dataType.asInstanceOf[StructType].fields.zipWithIndex.map { case (structField, i) =>
        (structField.name, GetStructField(structExpr, i))
      }
      val addOrReplaceFields: Seq[(String, Expression)] = addOrReplaceFieldNames.zip(addOrReplaceFieldExprs)
      val newFields: Seq[(String, Expression)] = loop(existingFields, addOrReplaceFields)
      val newFieldsRenamed: Seq[(String, Expression)] = {
        var fields = newFields
        existingFieldNames.zip(newFieldNames).foreach { case (existingName, newName) =>
          fields = fields.map {
            case (name, expr) if name == existingName => (newName, expr)
            case x => x
          }
        }
        fields
      }

      CreateNamedStruct(newFieldsRenamed.flatMap { case (name, expr) => Seq(Literal(name), expr) })
  }

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

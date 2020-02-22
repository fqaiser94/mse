package org.apache.spark.sql.catalyst

object Utilities {

  /**
    * Recursively loops through addOrReplaceFields, adding or replacing fields by FieldName.
    */
  def loop[V](existingFields: Seq[(String, V)], addOrReplaceFields: Seq[(String, V)]): Seq[(String, V)] = {
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

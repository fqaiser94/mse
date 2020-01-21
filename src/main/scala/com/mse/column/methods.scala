package com.mse.column

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AddField, DropFields, Expression, RenameField}

object methods {

  implicit class CustomColumnMethods(val col: Column) {

    private val expr = col.expr

    /**
      * An expression that adds/replaces a field by name in a `StructType`.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def withField(fieldName: String, fieldValue: Column): Column = withExpr {
      AddField(expr, fieldName, fieldValue.expr)
    }

    /**
      * An expression that drops given fields by name in a `StructType`.
      * This is a no-op if schema doesn't contain given field name(s).
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def dropFields(fieldNames: String*): Column = withExpr {
      DropFields(expr, fieldNames: _*)
    }

    /**
      * An expression that renames a field by name in a `StructType`.
      * This is a no-op if schema doesn't contain any field with existingFieldName.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def withFieldRenamed(existingFieldName: String, newFieldName: String): Column = withExpr {
      RenameField(expr, existingFieldName, newFieldName)
    }

    private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  }

}

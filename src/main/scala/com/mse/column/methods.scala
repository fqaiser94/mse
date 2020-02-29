package com.mse.column

import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AddFields, DropFields, Expression, RenameFields}
import org.apache.spark.sql.functions._

object methods {

  implicit def symbolToColumnWithCustomMethods(s: Symbol): ColumnWithCustomMethods = ColumnWithCustomMethods(new ColumnName(s.name))

  implicit class ColumnWithCustomMethods(val col: Column) {

    private val expr = col.expr

    /**
      * An expression that adds/replaces a field by name in a `StructType`.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def withField(fieldName: String, fieldValue: Column): Column = withExpr {
      AddFields(expr, fieldName, fieldValue.expr)
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
      * If there are multiple fields with existingFieldName, they will all be renamed.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def withFieldRenamed(existingFieldName: String, newFieldName: String): Column = withExpr {
      RenameFields(expr, existingFieldName, newFieldName)
    }

    private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  }

  /**
    * A convenience method for adding/replacing a field in a deeply nested struct.
    *
    * @param struct     : e.g. $"a.b.c" where a, b, and c are StructType columns and a is a top-level StructType Column and c is the StructType Column you want to add/replace a field in.
    * @param fieldName  : Field to add/replace in nestedStructType based on name.
    * @param fieldValue : Value to assign to fieldName
    * @return a copy the top-level nestedStructType column with field added/replaced
    */
  def add_struct_field(struct: String, fieldName: String, fieldValue: Column): Column = {
    update_struct(
      struct,
      col(struct).withField(fieldName, fieldValue)
    )
  }

  /**
    * A convenience method for dropping fields in a deeply nested struct.
    *
    * @param struct     : e.g. $"a.b.c" where a, b, and c are StructType columns and a is a top-level StructType Column and c is the StructType Column you want to drop fields from.
    * @param fieldNames : Field/s to drop in nestedStructType based on name.
    * @return a copy the top-level nestedStructType column with field/s dropped
    */
  def drop_struct_fields(struct: String, fieldNames: String*): Column = {
    update_struct(
      struct,
      col(struct).dropFields(fieldNames: _*)
    )
  }

  /**
    * A convenience method for renaming a field in a deeply nested struct.
    *
    * @param struct            : e.g. $"a.b.c" where a, b, and c are StructType columns and a is a top-level StructType Column and c is the StructType Column you want to rename a field in.
    * @param existingFieldName : The name of the field to rename.
    * @param newFieldName      : The name to give the field you want to rename.
    * @return a copy the top-level nestedStructTypeCol column with field/s renamed.
    */
  def rename_struct_field(struct: String, existingFieldName: String, newFieldName: String): Column = {
    update_struct(
      struct,
      col(struct).withFieldRenamed(existingFieldName, newFieldName)
    )
  }

  /**
    * A convenience method for adding/replacing a deeply nested struct.
    *
    * @param struct : e.g. $"a.b.c" where a, b, and c are StructType columns and a is a top-level StructType Column and c is the StructType Column you want to add/replace.
    * @param column : The value to assign to struct.
    */
  def update_struct(struct: String, column: Column): Column = {
    parseParentChildPairs(struct).foldRight(column) {
      case ((parentColName, childColName), childCol) =>
        col(parentColName).withField(childColName, childCol)
    }
  }

  /**
    * Given: "a.b.c.d.e"
    * Returns: Seq(("a", "b"), ("a.b", "c"), ("a.b.c", "d"))
    */
  private def parseParentChildPairs(columnName: String): Seq[(String, String)] = {
    val nameParts = UnresolvedAttribute.parseAttributeName(columnName)

    var result = Seq.empty[(String, String)]
    var i = 0
    while (i < nameParts.length - 1) {
      val parent = nameParts.slice(0, i + 1).map(x => s"`$x`").mkString(".")
      val child = nameParts(i + 1)
      result = result :+ (parent, child)
      i += 1
    }

    result
  }

}

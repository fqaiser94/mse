package com.github.fqaiser94.mse

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.LambdaFunction
import org.apache.spark.sql.catalyst.expressions.RenameFieldsByFunction
import org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable
import org.apache.spark.sql.catalyst.expressions.{AddFields, DropFields, Expression, Literal, RenameFields}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, ColumnName}

object methods {

  implicit def symbolToColumnWithCustomMethods(s: Symbol): ColumnWithCustomMethods = ColumnWithCustomMethods(new ColumnName(s.name))

  implicit class ColumnWithCustomMethods(val col: Column) {

    private val expr = col.expr

    /**
      * An expression that adds/replaces a field by name in a `StructType`.
      * If schema contains multiple fields with fieldName, they will all be replaced with fieldValue.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def withField(fieldName: String, fieldValue: Column): Column = withExpr {
      AddFields(expr :: Literal(fieldName) :: fieldValue.expr :: Nil)
    }

    /**
      * An expression that drops fields by name in a `StructType`.
      * This is a no-op if schema doesn't contain given field names.
      * If schema contains multiple fields matching any one of the given fieldNames, they will all be dropped.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def dropFields(fieldNames: String*): Column = withExpr {
      DropFields(expr +: fieldNames.toList.map(Literal(_)))
    }

    /**
      * An expression that renames a field by name in a `StructType`.
      * This is a no-op if schema doesn't contain any field with existingFieldName.
      * If schema contains multiple fields with existingFieldName, they will all be renamed to newFieldName.
      *
      * @group expr_ops
      * @since 2.4.4
      */
    def withFieldRenamed(existingFieldName: String, newFieldName: String): Column = withExpr {
      RenameFields(expr :: Literal(existingFieldName) :: Literal(newFieldName) :: Nil)
    }

    def withFieldRenamedByFn(renameFn: Column => Column): Column = withExpr {
      RenameFieldsByFunction(expr, createLambdaForRenameFn(renameFn))
    }

    /**
      * Adapted from [[org.apache.spark.sql.functions.createLambda]]
      *
      * @param f the function
      * @return a lambda encapsulating the given function
      */
    private def createLambdaForRenameFn(f: Column => Column) = {
      val x = UnresolvedNamedLambdaVariable(Seq("x"))
      val function = f(withExpr(x)).expr
      LambdaFunction(function, Seq(x))
    }

    private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  }

  /**
   * A convenience method for adding/replacing a field by name inside a deeply nested struct.
   *
   * @param nestedStruct : e.g. "a.b.c" where a, b, and c are StructType columns and a is a top-level StructType Column and c is the StructType Column to add/replace field in.
   * @param fieldName    : The name of the StructField to add (if it does not already exist) or replace (if it already exists).
   * @param fieldValue   : The value to assign to fieldName.
   * @return a copy the top-level struct column (a) with field added/replaced.
   */
  def add_struct_field(nestedStruct: String, fieldName: String, fieldValue: Column): Column = {
    parseParentChildPairs(s"$nestedStruct.`$fieldName`").foldRight(fieldValue) {
      case ((parentColName, childColName), childCol) =>
        col(parentColName).withField(childColName, childCol)
    }
  }

  /**
    * Given: "a.b.c.d"
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

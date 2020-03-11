from pyspark import SparkContext
from pyspark.sql import Column


def __withField(self: Column, fieldName: str, fieldValue: Column):
    """
    An expression that adds/replaces a field by name in a `StructType`.
    If schema contains multiple fields with fieldName, they will all be replaced with fieldValue.
    """
    sc = SparkContext._active_spark_context
    _columnWithCustomMethods = sc._jvm.com.github.fqaiser94.mse.methods.ColumnWithCustomMethods(self._jc)
    _column = _columnWithCustomMethods.withField(fieldName, fieldValue._jc)
    return Column(_column)


def __dropFields(self: Column, *fieldNames: str):
    """
    An expression that drops fields by name in a `StructType`.
    This is a no-op if schema doesn't contain given field names.
    If schema contains multiple fields matching any one of the given fieldNames, they will all be dropped.
    """
    sc = SparkContext._active_spark_context
    _columnWithCustomMethods = sc._jvm.com.github.fqaiser94.mse.methods.ColumnWithCustomMethods(self._jc)
    _fieldNames = sc._jvm.PythonUtils.toSeq(fieldNames)
    _column = _columnWithCustomMethods.dropFields(_fieldNames)
    return Column(_column)


def __withFieldRenamed(self: Column, existingFieldName: str, newFieldName: str):
    """
    An expression that renames a field by name in a `StructType`.
    This is a no-op if schema doesn't contain any field with existingFieldName.
    If schema contains multiple fields with existingFieldName, they will all be renamed to newFieldName.
    """
    sc = SparkContext._active_spark_context
    _columnWithCustomMethods = sc._jvm.com.github.fqaiser94.mse.methods.ColumnWithCustomMethods(self._jc)
    _column = _columnWithCustomMethods.withFieldRenamed(existingFieldName, newFieldName)
    return Column(_column)


Column.withField = __withField
Column.withFieldRenamed = __withFieldRenamed
Column.dropFields = __dropFields

def add_struct_field(nestedStruct: str, fieldName: str, fieldValue: Column):
    """
    A convenience method for adding/replacing a field by name inside a deeply nested struct.

    :param nestedStruct : e.g. "a.b.c" where a, b, and c are StructType columns and a is a top-level StructType Column and c is the StructType Column to add/replace field in.
    :param fieldName    : The name of the StructField to add (if it does not already exist) or replace (if it already exists).
    :param fieldValue   : The value to assign to fieldName.
    :return: a copy the top-level struct column (a) with field added/replaced.
    """
    sc = SparkContext._active_spark_context
    _add_struct_field = sc._jvm.com.github.fqaiser94.mse.methods.add_struct_field
    _column = _add_struct_field(nestedStruct, fieldName, fieldValue._jc)
    return Column(_column)

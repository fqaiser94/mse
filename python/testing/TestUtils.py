from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import unittest

def check_answer(result: DataFrame, expected_value: list, expected_schema: StructType):
    tc = unittest.TestCase('__init__')
    tc.assertEqual(result.collect(), expected_value)
    tc.assertEqual(result.schema, expected_schema)

def updated(self: StructType, idx: int, element: StructField):
    fields_list = self.fields.copy()
    fields_list[idx] = element
    return StructType(fields_list)

StructType.updated = updated
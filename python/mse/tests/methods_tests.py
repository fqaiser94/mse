from pyspark import *
from pyspark.sql import *
from pyspark.sql.utils import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from mse.methods import *
from testing.TestUtils import *

from testing.ReusedSQLTestCase import ReusedSQLTestCase


class MethodsTests(ReusedSQLTestCase):

    def test_withField(self):
        non_struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(1)]),
            StructType([StructField("a", IntegerType())]))

        struct_level1_schema = StructType([
            StructField("a", IntegerType()),
            StructField("b", IntegerType()),
            StructField("c", IntegerType())])

        struct_level1_with_new_field_schema = StructType(
            struct_level1_schema.fields + [StructField("d", IntegerType(), nullable=False)])

        struct_level1_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(Row(1, 1, 1))]),
            StructType([StructField("a", struct_level1_schema)]))

        null_struct_level1_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(None)]),
            StructType([StructField("a", struct_level1_schema)]))

        struct_level2_schema = StructType([StructField("a", struct_level1_schema)])

        struct_level2_with_new_field_schema = StructType([StructField("a", struct_level1_with_new_field_schema)])

        struct_level2_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(Row(Row(1, 1, 1)))]),
            StructType([StructField("a", struct_level2_schema)]))

        null_struct_level2_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(None)]),
            StructType([StructField("a", struct_level2_schema)]))

        struct_level3_schema = \
            StructType([
                StructField("a", struct_level1_schema),
                StructField("b", StructType([
                    StructField("a", struct_level1_schema),
                    StructField("b", struct_level1_schema)]))])

        struct_level3_with_new_field_schema = \
            StructType([
                StructField("a", struct_level1_schema),
                StructField("b", StructType([
                    StructField("a", struct_level1_with_new_field_schema),
                    StructField("b", struct_level1_schema)]))])

        struct_level3_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(Row(Row(1, 2, 3), Row(Row(4, None, 6), Row(7, 8, 9))))]),
            StructType([StructField("a", struct_level3_schema)]))

        null_struct_level3_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(None)]),
            StructType([StructField("a", struct_level3_schema)]))

        with self.subTest("throw error if withField is called on a column that is not struct dataType"):
            self.assertRaisesRegex(
                AnalysisException,
                "struct should be struct data type. struct is integer",
                lambda: non_struct_df.withColumn("a", col("a").withField("a", lit(2))).collect())

        with self.subTest("throw error if null fieldName supplied"):
            self.assertRaisesRegex(
                AnalysisException,
                "fieldNames cannot contain null",
                lambda: struct_level1_df.withColumn("a", col("a").withField(None, lit(2))).collect())

        with self.subTest("add new field to struct"):
            check_answer(
                struct_level1_df.withColumn("a", col("a").withField("d", lit(2))),
                [Row(Row(1, 1, 1, 2))],
                StructType([StructField("a", struct_level1_with_new_field_schema)]))

        with self.subTest("add multiple new fields to struct"):
            check_answer(
                struct_level1_df.withColumn("a", col("a").withField("d", lit(2)).withField("e", lit(3))),
                [Row(Row(1, 1, 1, 2, 3))],
                StructType([StructField("a", StructType(
                    struct_level1_schema.fields + [StructField("d", IntegerType(), nullable=False),
                                                   StructField("e", IntegerType(), nullable=False)]))]))

        with self.subTest("replace field in struct"):
            check_answer(
                struct_level1_df.withColumn("a", col("a").withField("b", lit(2))),
                [Row(Row(1, 2, 1))],
                StructType([StructField("a", struct_level1_schema.updated(1, StructField("b", IntegerType(),
                                                                                         nullable=False)))]))

        with self.subTest("replace multiple fields in struct"):
            check_answer(
                struct_level1_df.withColumn("a", col("a").withField("a", lit(2)).withField("b", lit(2))),
                [Row(Row(2, 2, 1))],
                StructType([
                    StructField("a", struct_level1_schema
                                .updated(0, StructField("a", IntegerType(), nullable=False))
                                .updated(1, StructField("b", IntegerType(), nullable=False)))]))

        with self.subTest("add and replace fields in struct"):
            check_answer(
                struct_level1_df.withColumn("a", col("a").withField("b", lit(2)).withField("d", lit(2))),
                [Row(Row(1, 2, 1, 2))],
                StructType([StructField("a", struct_level1_with_new_field_schema.updated(1, StructField("b", IntegerType(), nullable=False)))]))

        with self.subTest("add new field to nested struct"):
            check_answer(
                struct_level2_df.withColumn("a", col("a").withField("a", col("a.a").withField("d", lit(2)))),
                [Row(Row(Row(1, 1, 1, 2)))],
                StructType([StructField("a", struct_level2_with_new_field_schema)]))

        with self.subTest("replace field in nested struct"):
            check_answer(
                struct_level2_df.withColumn("a", col("a").withField("a", col("a.a").withField("b", lit(2)))),
                [Row(Row(Row(1, 2, 1)))],
                StructType([
                    StructField("a", StructType([
                        StructField("a", struct_level1_schema.updated(1, StructField("b", IntegerType(), nullable=False)))]))]))

        with self.subTest("add field to deeply nested struct"):
            check_answer(
                struct_level3_df.withColumn("a", col("a").withField(
                    "b", col("a.b").withField(
                        "a", col("a.b.a").withField(
                            "d", lit(0))))),
                [Row(Row(Row(1, 2, 3), Row(Row(4, None, 6, 0), Row(7, 8, 9))))],
                StructType([StructField("a", struct_level3_with_new_field_schema)]))

        with self.subTest("replace field in deeply nested struct"):
            check_answer(
                struct_level3_df.withColumn("a", col("a").withField(
                    "b", col("a.b").withField(
                        "a", col("a.b.a").withField(
                            "b", lit(5))))),
                [Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9))))],
                StructType([
                    StructField("a", StructType([
                        StructField("a", struct_level1_schema),
                        StructField("b", StructType([
                            StructField("a", struct_level1_schema.updated(1, StructField("b", IntegerType(),nullable=False))),
                            StructField("b", struct_level1_schema)]))]))]))

        with self.subTest("return null if struct is null"):
            check_answer(
                null_struct_level1_df.withColumn("a", col("a").withField("d", lit(2))),
                [Row(None)],
                StructType([StructField("a", struct_level1_with_new_field_schema)]))

            check_answer(
                null_struct_level2_df.withColumn("a", col("a").withField(
                    "a", col("a.a").withField(
                        "d", lit(2)))),
                [Row(None)],
                StructType([StructField("a", struct_level2_with_new_field_schema)]))

            check_answer(
                null_struct_level3_df.withColumn("a", col("a").withField(
                    "b", col("a.b").withField(
                        "a", col("a.b.a").withField(
                            "d", lit(0))))),
                [Row(None)],
                StructType([StructField("a", struct_level3_with_new_field_schema)]))

    def test_withFieldRenamed(self):
        non_struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(1)]),
            StructType([StructField("a", IntegerType())]))

        struct_schema = StructType([
            StructField("a", IntegerType()),
            StructField("b", IntegerType()),
            StructField("c", IntegerType()),
            StructField("c", IntegerType())])

        struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(Row(1, 2, 3, 4))]),
            StructType([StructField("a", struct_schema)]))

        null_struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(None)]),
            StructType([StructField("a", struct_schema)]))

        with self.subTest("throw error if withFieldRenamed is called on a column that is not struct dataType"):
            self.assertRaisesRegex(
                AnalysisException,
                "struct should be struct data type. struct is integer",
                lambda: non_struct_df.withColumn("a", col("a").withFieldRenamed("a", "z")))

        with self.subTest("throw error if null existingFieldName supplied"):
            self.assertRaisesRegex(
                AnalysisException,
                "existingFieldName cannot be null",
                lambda: struct_df.withColumn("a", col("a").withFieldRenamed(None, "z")))

        with self.subTest("throw error if null newFieldName supplied"):
            self.assertRaisesRegex(
                AnalysisException,
                "newFieldName cannot be null",
                lambda: struct_df.withColumn("a", col("a").withFieldRenamed("a", None)))

        with self.subTest("rename field in struct"):
            expected_value = [Row(Row(1, 2, 3, 4))]
            expected_schema = StructType([
                StructField("a", StructType([
                    StructField("a", IntegerType()),
                    StructField("y", IntegerType()),
                    StructField("c", IntegerType()),
                    StructField("c", IntegerType())]))])

            check_answer(
                struct_df.withColumn("a", col("a").withFieldRenamed("b", "y")),
                expected_value,
                expected_schema)

            check_answer(
                struct_df.withColumn("a", col("a").withFieldRenamed("b", "y")),
                expected_value,
                expected_schema)

        with self.subTest("rename multiple fields in struct"):
            check_answer(
                struct_df.withColumn("a", col("a").withFieldRenamed("c", "x")),
                [Row(Row(1, 2, 3, 4))],
                StructType([
                    StructField("a", StructType([
                        StructField("a", IntegerType()),
                        StructField("b", IntegerType()),
                        StructField("x", IntegerType()),
                        StructField("x", IntegerType())]))]))

        with self.subTest("don't rename anything if no field exists with existingFieldName in struct"):
            check_answer(
                struct_df.withColumn("a", col("a").withFieldRenamed("d", "x")),
                [Row(Row(1, 2, 3, 4))],
                StructType([
                    StructField("a", StructType([
                        StructField("a", IntegerType()),
                        StructField("b", IntegerType()),
                        StructField("c", IntegerType()),
                        StructField("c", IntegerType())]))]))

        with self.subTest("return null for if struct is null"):
            check_answer(
                null_struct_df.withColumn("a", col("a").withFieldRenamed("b", "y")),
                [Row(None)],
                StructType([
                    StructField("a", StructType([
                        StructField("a", IntegerType()),
                        StructField("y", IntegerType()),
                        StructField("c", IntegerType()),
                        StructField("c", IntegerType())]))]))

    def test_dropFields(self):
        non_struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(1)]),
            StructType([StructField("a", IntegerType())]))

        struct_schema = StructType([
            StructField("a", IntegerType()),
            StructField("b", IntegerType()),
            StructField("c", IntegerType())])

        struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(Row(1, 2, 3))]),
            StructType([StructField("a", struct_schema)]))

        null_struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(None)]),
            StructType([
                StructField("a", StructType([
                    StructField("a", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("c", IntegerType())]))]))

        with self.subTest("throw error if withField is called on a column that is not struct dataType"):
            self.assertRaisesRegex(
                AnalysisException,
                "struct should be struct data type. struct is integer",
                lambda: non_struct_df.withColumn("a", col("a").dropFields("a")))

        with self.subTest("throw error if null fieldName supplied"):
            self.assertRaisesRegex(
                AnalysisException,
                "fieldNames cannot contain null",
                lambda: struct_df.withColumn("a", col("a").dropFields(None)))

        with self.subTest("drop field in struct"):
            expected_value = [Row(Row(1, 3))]
            expected_schema = StructType([
                StructField("a", StructType([
                    StructField("a", IntegerType()),
                    StructField("c", IntegerType())]))])

            check_answer(
                struct_df.withColumn("a", col("a").dropFields("b")),
                expected_value,
                expected_schema)

            check_answer(
                struct_df.withColumn("a", col("a").dropFields("b")),
                expected_value,
                expected_schema)

        with self.subTest("drop multiple fields in struct"):
            check_answer(
                struct_df.withColumn("a", col("a").dropFields("c", "a")),
                [Row(Row(2))],
                StructType([
                    StructField("a", StructType([
                        StructField("b", IntegerType())]))]))

        with self.subTest("drop all fields in struct"):
            check_answer(
                struct_df.withColumn("a", col("a").dropFields("c", "a", "b")),
                [Row(Row())],
                StructType([StructField("a", StructType())]))

        with self.subTest("drop multiple fields in struct with multiple dropFields calls"):
            check_answer(
                struct_df.withColumn("a", col("a").dropFields("c").dropFields("a")),
                [Row(Row(2))],
                StructType([
                    StructField("a", StructType([
                        StructField("b", IntegerType())]))]))

        with self.subTest("return null for if struct is null"):
            check_answer(
                null_struct_df.withColumn("a", col("a").dropFields("c")),
                [Row(None)],
                StructType([
                    StructField("a", StructType([
                        StructField("a", IntegerType()),
                        StructField("b", IntegerType())]))]))

    def test_add_struct_field(self):
        struct_schema = StructType([
            StructField("a", StructType([
                StructField("important.metric", StructType([
                    StructField("a", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("c", IntegerType())])),
                StructField("b", StructType([
                    StructField("a", StructType([
                        StructField("a", IntegerType()),
                        StructField("b", IntegerType()),
                        StructField("c", IntegerType())])),
                    StructField("b", StructType([
                        StructField("a", IntegerType()),
                        StructField("b", IntegerType()),
                        StructField("c", IntegerType())]))]))]))])

        struct_df = self.spark.createDataFrame(
            self.sc.parallelize([Row(Row(Row(1, 2, 3), Row(Row(4, None, 6), Row(7, 8, 9))))]),
            struct_schema)

        with self.subTest("add field to deeply nested struct"):
            check_answer(
                struct_df.withColumn("a", add_struct_field("a.b.a", "d", lit("hello"))),
                [Row(Row(Row(1, 2, 3), Row(Row(4, None, 6, "hello"), Row(7, 8, 9))))],
                StructType([
                    StructField("a", StructType([
                        StructField("important.metric", StructType([
                            StructField("a", IntegerType()),
                            StructField("b", IntegerType()),
                            StructField("c", IntegerType())])),
                        StructField("b", StructType([
                            StructField("a", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType()),
                                StructField("c", IntegerType()),
                                StructField("d", StringType(), nullable=False)])),
                            StructField("b", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType()),
                                StructField("c", IntegerType())]))]))]))]))

        with self.subTest("replace field in deeply nested struct"):
            check_answer(
                struct_df.withColumn("a", add_struct_field("a.b.a", "b", lit(5))),
                [Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9))))],
                StructType([
                    StructField("a", StructType([
                        StructField("important.metric", StructType([
                            StructField("a", IntegerType()),
                            StructField("b", IntegerType()),
                            StructField("c", IntegerType())])),
                        StructField("b", StructType([
                            StructField("a", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType(), nullable=False),
                                StructField("c", IntegerType())])),
                            StructField("b", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType()),
                                StructField("c", IntegerType())]))]))]))]))

        with self.subTest("drop fields in deeply nested struct"):
            check_answer(
                struct_df.withColumn("a", add_struct_field("a.b", "a", col("a.b.a").dropFields("b", "c"))),
                [Row(Row(Row(1, 2, 3), Row(Row(4), Row(7, 8, 9))))],
                StructType([
                    StructField("a", StructType([
                        StructField("important.metric", StructType([
                            StructField("a", IntegerType()),
                            StructField("b", IntegerType()),
                            StructField("c", IntegerType())])),
                        StructField("b", StructType([
                            StructField("a", StructType([
                                StructField("a", IntegerType())])),
                            StructField("b", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType()),
                                StructField("c", IntegerType())]))]))]))]))

        with self.subTest("rename field in deeply nested struct"):
            check_answer(
                struct_df.withColumn("a", add_struct_field("a.b", "a", col("a.b.a").withFieldRenamed("b", "z"))),
                [Row(Row(Row(1, 2, 3), Row(Row(4, None, 6), Row(7, 8, 9))))],
                StructType([
                    StructField("a", StructType([
                        StructField("important.metric", StructType([
                            StructField("a", IntegerType()),
                            StructField("b", IntegerType()),
                            StructField("c", IntegerType())])),
                        StructField("b", StructType([
                            StructField("a", StructType([
                                StructField("a", IntegerType()),
                                StructField("z", IntegerType()),
                                StructField("c", IntegerType())])),
                            StructField("b", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType()),
                                StructField("c", IntegerType())]))]))]))]))

        with self.subTest("add_struct_field should throw exception if field does not exist"):
            self.assertRaisesRegex(
                AnalysisException,
                "cannot resolve '`xxx`' given input columns: \\[a\\]",
                lambda: struct_df.withColumn("a", add_struct_field("xxx.b.a", "d", lit("hello"))).collect())

            self.assertRaisesRegex(
                AnalysisException,
                "No such struct field xxx in important.metric, b",
                lambda: struct_df.withColumn("a", add_struct_field("a.xxx.a", "d", lit("hello"))).collect())

            self.assertRaisesRegex(
                AnalysisException,
                "No such struct field xxx in a, b",
                lambda: struct_df.withColumn("a", add_struct_field("a.b.xxx", "d", lit("hello"))).collect())

            self.assertRaisesRegex(
                AnalysisException,
                "No such struct field xxx in a, b, c;",
                lambda: struct_df.withColumn("a", add_struct_field("a.b.a.xxx", "d", lit("hello"))).collect())

        with self.subTest("add_struct_field should handle column names with dots appropriately"):
            with self.subTest("should throw exception if struct column names with dots are not properly escaped"):
                self.assertRaisesRegex(
                    AnalysisException,
                    "No such struct field important in important.metric, b",
                    lambda: struct_df.withColumn("a", add_struct_field("a.important.metric", "b", lit(100))).collect())

            with self.subTest("should succeed if struct column names with dots are properly escaped"):
                check_answer(
                    struct_df.withColumn("a", add_struct_field("a.`important.metric`", "b", lit(100))),
                    [Row(Row(Row(1, 100, 3), Row(Row(4, None, 6), Row(7, 8, 9))))],
                    StructType([
                        StructField("a", StructType([
                            StructField("important.metric", StructType([
                                StructField("a", IntegerType()),
                                StructField("b", IntegerType(), nullable=False),
                                StructField("c", IntegerType())])),
                            StructField("b", StructType([
                                StructField("a", StructType([
                                    StructField("a", IntegerType()),
                                    StructField("b", IntegerType()),
                                    StructField("c", IntegerType())])),
                                StructField("b", StructType([
                                    StructField("a", IntegerType()),
                                    StructField("b", IntegerType()),
                                    StructField("c", IntegerType())]))]))]))]))

    def test_fail(self):
        self.assertEqual(1, 2)


if __name__ == "__main__":
    import unittest

    unittest.main()

package com.mse.column

import methods._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.scalatest.Matchers

class methodsTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private def checkAnswer(df: => DataFrame,
                          expectedAnswer: Seq[Row],
                          expectedSchema: StructType): Unit = {

    assert(df.schema == expectedSchema)
    checkAnswer(df, expectedAnswer)
  }

  test("withField") {
    val structLevel1 = spark.createDataFrame(sparkContext.parallelize(
      Row(Row(1, 1, 1)) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))))))

    val structLevel2 = spark.createDataFrame(sparkContext.parallelize(
      Row(Row(Row(1, 1, 1))) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))))))))))

    val structLevel3 = spark.createDataFrame(sparkContext.parallelize(
      Seq(Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))))),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))

    // throw error if withField is called on a column that is not struct dataType
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".withField("a", lit(2)))
    }.getMessage should include("struct should be struct data type. struct is integer")

    // throw error if null fieldName supplied
    intercept[AnalysisException] {
      structLevel1.withColumn("a", $"a".withField(null, lit(2)))
    }.getMessage should include("fieldName cannot be null")

    // add new field to struct
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(2))),
      Row(Row(1, 1, 1, 2)) :: Nil)

    // replace field in struct
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(2))),
      Row(Row(1, 2, 1)) :: Nil)

    // add new field to nested struct
    checkAnswer(
      structLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "d", lit(2)))),
      Row(Row(Row(1, 1, 1, 2))) :: Nil)

    // replace field in nested struct
    checkAnswer(
      structLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "b", lit(2)))),
      Row(Row(Row(1, 2, 1))) :: Nil)

    // add field to deeply nested struct
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "d", lit("hello"))))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, "hello"), Row(7, 8, 9)))) :: Nil)

    // replace field in deeply nested struct
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "b", lit(5))))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil)
  }

  test("withFieldRenamed") {
    val df = spark.createDataFrame(sparkContext.parallelize(
      Row(Row(1, 2, 3, 4)) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
          StructField("c", IntegerType)))))))

    // throw error if withField is called on a column that is not struct dataType
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".withFieldRenamed("a", "z"))
    }.getMessage should include("struct should be struct data type. struct is integer")

    // throw error if null existingFieldName supplied
    intercept[AnalysisException] {
      df.withColumn("a", $"a".withFieldRenamed(null, "z"))
    }.getMessage should include("existingFieldName cannot be null")

    // throw error if null existingFieldName supplied
    intercept[AnalysisException] {
      df.withColumn("a", $"a".withFieldRenamed("a", null))
    }.getMessage should include("newFieldName cannot be null")

    // rename field in struct
    checkAnswer(
      df.withColumn("a", $"a".withFieldRenamed("b", "y")),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("y", IntegerType),
          StructField("c", IntegerType),
          StructField("c", IntegerType)))))))

    // rename multiple fields in struct
    checkAnswer(
      df.withColumn("a", $"a".withFieldRenamed("c", "x")),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("x", IntegerType),
          StructField("x", IntegerType)))))))

    // don't rename anything if no field exists with existingFieldName in struct
    checkAnswer(
      df.withColumn("a", $"a".withFieldRenamed("d", "x")),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
          StructField("c", IntegerType)))))))
  }

  test("dropFields") {
    val df = spark.createDataFrame(sparkContext.parallelize(
      Row(Row(1, 2, 3)) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))))))

    // throw error if withField is called on a column that is not struct dataType
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".dropFields("a"))
    }.getMessage should include("struct should be struct data type. struct is integer")

    // throw error if null fieldName supplied
    intercept[AnalysisException] {
      df.withColumn("a", $"a".dropFields(null))
    }.getMessage should include("fieldNames cannot contain null")

    // drop field in struct
    checkAnswer(
      df.withColumn("a", $"a".dropFields("b")),
      Row(Row(1, 3)) :: Nil)

    // drop multiple fields in struct
    checkAnswer(
      df.withColumn("a", $"a".dropFields("c", "a")),
      Row(Row(2)) :: Nil)

    // drop all fields in struct
    checkAnswer(
      df.withColumn("a", $"a".dropFields("c", "a", "b")),
      Row(null) :: Nil)
  }

}

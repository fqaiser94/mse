package com.mse.column

import com.mse.column.methods._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.scalatest.Matchers

class withFieldTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private lazy val structLevel1 = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 1, 1)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))

  private lazy val structLevel2 = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(Row(1, 1, 1))) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType))))))))))

  private lazy val structLevel3 = spark.createDataFrame(sparkContext.parallelize(
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

  test("throw error if withField is called on a column that is not struct dataType") {
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".withField("a", lit(2)))
    }.getMessage should include("struct should be struct data type. struct is integer")
  }

  test("throw error if null fieldName supplied") {
    intercept[AnalysisException] {
      structLevel1.withColumn("a", $"a".withField(null, lit(2)))
    }.getMessage should include("fieldName cannot be null")
  }

  test("add new field to struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(2))),
      Row(Row(1, 1, 1, 2)) :: Nil)
  }

  test("replace field in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(2))),
      Row(Row(1, 2, 1)) :: Nil)
  }

  test("add new field to nested struct") {
    checkAnswer(
      structLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "d", lit(2)))),
      Row(Row(Row(1, 1, 1, 2))) :: Nil)
  }

  test("replace field in nested struct") {
    checkAnswer(
      structLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "b", lit(2)))),
      Row(Row(Row(1, 2, 1))) :: Nil)
  }

  test("add field to deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "d", lit("hello"))))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, "hello"), Row(7, 8, 9)))) :: Nil)
  }

  test("replace field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "b", lit(5))))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil)
  }
}

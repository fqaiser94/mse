package com.mse.column

import com.mse.column.methods._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.scalatest.Matchers

class withFieldRenamedTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private def checkAnswer(df: => DataFrame,
                          expectedAnswer: Seq[Row],
                          expectedSchema: StructType): Unit = {

    assert(df.schema == expectedSchema)
    checkAnswer(df, expectedAnswer)
  }

  private lazy val df = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3, 4)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType),
        StructField("c", IntegerType)))))))

  test("throw error if withField is called on a column that is not struct dataType") {
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".withFieldRenamed("a", "z"))
    }.getMessage should include("struct should be struct data type. struct is integer")
  }

  test("throw error if null existingFieldName supplied") {
    intercept[AnalysisException] {
      df.withColumn("a", $"a".withFieldRenamed(null, "z"))
    }.getMessage should include("existingFieldName cannot be null")
  }

  test("throw error if null newFieldName supplied") {
    intercept[AnalysisException] {
      df.withColumn("a", $"a".withFieldRenamed("a", null))
    }.getMessage should include("newFieldName cannot be null")
  }

  test("rename field in struct") {
    checkAnswer(
      df.withColumn("a", $"a".withFieldRenamed("b", "y")),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("y", IntegerType),
          StructField("c", IntegerType),
          StructField("c", IntegerType)))))))
  }

  test("rename multiple fields in struct") {
    checkAnswer(
      df.withColumn("a", $"a".withFieldRenamed("c", "x")),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("x", IntegerType),
          StructField("x", IntegerType)))))))
  }

  test("don't rename anything if no field exists with existingFieldName in struct") {
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
}

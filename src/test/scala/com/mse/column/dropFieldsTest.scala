package com.mse.column

import com.mse.column.methods._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.scalatest.Matchers

class dropFieldsTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private lazy val df = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))

  test("throw error if withField is called on a column that is not struct dataType") {
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".dropFields("a"))
    }.getMessage should include("struct should be struct data type. struct is integer")
  }

  test("throw error if null fieldName supplied") {
    intercept[AnalysisException] {
      df.withColumn("a", $"a".dropFields(null))
    }.getMessage should include("fieldNames cannot contain null")
  }

  test("drop field in struct") {
    checkAnswer(
      df.withColumn("a", $"a".dropFields("b")),
      Row(Row(1, 3)) :: Nil)
  }

  test("drop multiple fields in struct") {
    checkAnswer(
      df.withColumn("a", $"a".dropFields("c", "a")),
      Row(Row(2)) :: Nil)
  }

  test("drop all fields in struct") {
    checkAnswer(
      df.withColumn("a", $"a".dropFields("c", "a", "b")),
      Row(null) :: Nil)
  }
}

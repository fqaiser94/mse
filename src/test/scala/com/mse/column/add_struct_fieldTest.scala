package com.mse.column

import com.mse.column.methods._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.scalatest.Matchers

class add_struct_fieldTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private lazy val structLevel3 = spark.createDataFrame(sparkContext.parallelize(
    Seq(Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))))),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("important.metric", StructType(Seq(
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

  private def checkAnswer(df: => DataFrame,
                          expectedAnswer: Seq[Row],
                          expectedSchema: StructType): Unit = {

    assert(df.schema == expectedSchema)
    checkAnswer(df, expectedAnswer)
  }

  test("add field to deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.b.a", "d", lit("hello"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, "hello"), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("important.metric", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType),
              StructField("d", StringType, nullable = false)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))
  }

  test("replace field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.b.a", "b", lit(5))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("important.metric", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType, nullable = false),
              StructField("c", IntegerType)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))
  }

  test("drop fields in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.b", "a", $"a.b.a".dropFields("b", "c"))),
      Row(Row(Row(1, 2, 3), Row(Row(4), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("important.metric", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))
  }

  test("rename field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.b", "a", $"a.b.a".withFieldRenamed("b", "z"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("important.metric", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType),
              StructField("z", IntegerType),
              StructField("c", IntegerType)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))
  }

  test("should throw exception if field does not exist") {
    intercept[AnalysisException] {
      structLevel3.withColumn("a", add_struct_field("xxx.b.a", "d", lit("hello"))).collect
    }.getMessage() should include("cannot resolve '`xxx`' given input columns: [a]")

    intercept[AnalysisException] {
      structLevel3.withColumn("a", add_struct_field("a.xxx.a", "d", lit("hello"))).collect
    }.getMessage() should include("No such struct field xxx in important.metric, b")

    intercept[AnalysisException] {
      structLevel3.withColumn("a", add_struct_field("a.b.xxx", "d", lit("hello"))).collect
    }.getMessage() should include("No such struct field xxx in a, b")

    intercept[AnalysisException] {
      structLevel3.withColumn("a", add_struct_field("a.b.a.xxx", "d", lit("hello"))).collect
    }.getMessage should include("No such struct field xxx in a, b, c;")
  }

  test("should handle column names with dots that are properly escaped using backticks") {
    intercept[AnalysisException] {
      structLevel3.withColumn("a", add_struct_field("a.important.metric", "b", lit(100))).collect
    }.getMessage should include("No such struct field important in important.metric, b")

    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.`important.metric`", "b", lit(100))),
      Row(Row(Row(1, 100, 3), Row(Row(4, null, 6), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("important.metric", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType, nullable = false),
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
  }
}

package com.mse.column

import com.mse.column.methods._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.scalatest.Matchers

class methodsTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

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

  private def checkAnswer(df: => DataFrame,
                          expectedAnswer: Seq[Row],
                          expectedSchema: StructType): Unit = {

    assert(df.schema == expectedSchema)
    checkAnswer(df, expectedAnswer)
  }

  test("add field to deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.b.a", "d", lit("hello"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, "hello"), Row(7, 8, 9)))) :: Nil)
  }

  test("replace field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", add_struct_field("a.b.a", "b", lit(5))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil)
  }

  test("drop field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", drop_struct_fields("a.b.a", "b")),
      Row(Row(Row(1, 2, 3), Row(Row(4, 6), Row(7, 8, 9)))) :: Nil)
  }

  test("drop fields in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", drop_struct_fields("a.b.a", "b", "c")),
      Row(Row(Row(1, 2, 3), Row(Row(4), Row(7, 8, 9)))) :: Nil)
  }

  test("rename field in deeply nested struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
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
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", rename_struct_field("a.b.a", "b", "z")),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))) :: Nil,
      expectedSchema)
  }

  test("add_struct_field, rename_struct_field, and drop_struct_fields should throw exception if field does not exist") {
    Seq[String => Column](
      add_struct_field(_, "d", lit("hello")),
      rename_struct_field(_, "b", "z"),
      drop_struct_fields(_, "c")
    ).foreach { func =>
      intercept[AnalysisException] {
        structLevel3.withColumn("a", func("xxx.b.a")).show(false)
      }.getMessage() should include("cannot resolve '`xxx`' given input columns: [a]")

      intercept[AnalysisException] {
        structLevel3.withColumn("a", func("a.xxx.a")).show(false)
      }.getMessage() should include("No such struct field xxx in a, b")

      intercept[AnalysisException] {
        structLevel3.withColumn("a", func("a.b.xxx")).show(false)
      }.getMessage() should include("No such struct field xxx in a, b")

      intercept[AnalysisException] {
        structLevel3.withColumn("a", func("a.b.a.xxx")).show(false)
      }.getMessage should include("No such struct field xxx in a, b, c;")
    }
  }

  test("add field to deeply nested struct using update_struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))),
        StructField("b", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType),
            StructField("d", StringType, false)))),
          StructField("b", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a.d", lit("hello"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, "hello"), Row(7, 8, 9)))) :: Nil,
      expectedSchema
    )

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a", $"a.b.a".withField("d", lit("hello")))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, "hello"), Row(7, 8, 9)))) :: Nil,
      expectedSchema
    )
  }

  test(s"replace field in deeply nested struct using update_struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))),
        StructField("b", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType, false),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a.b", lit(5))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil,
      expectedSchema)

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a", $"a.b.a".withField("b", lit(5)))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil,
      expectedSchema)
  }

  test("drop field in deeply nested struct using update_struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))),
        StructField("b", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("c", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a", $"a.b.a".dropFields("b"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 6), Row(7, 8, 9)))) :: Nil,
      expectedSchema)
  }

  test("drop fields in deeply nested struct using update_struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))),
        StructField("b", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType)))),
          StructField("b", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a", $"a.b.a".dropFields("b", "c"))),
      Row(Row(Row(1, 2, 3), Row(Row(4), Row(7, 8, 9)))) :: Nil,
      expectedSchema)
  }

  test("rename field in deeply nested struct using update_struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
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
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct("a.b.a", $"a.b.a".withFieldRenamed("b", "z"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))) :: Nil,
      expectedSchema)
  }

  test("replace field, add field, rename field, and drop field in deeply nested struct using update_struct") {
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))),
        StructField("b", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("z", IntegerType),
            StructField("c", StringType, false),
            StructField("d", StringType, false)))),
          StructField("b", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct(
        "a.b.a",
        $"a.b.a".withField("c", lit("hello")).withField("d", lit("world")).withFieldRenamed("a", "z").dropFields("b"))),
      Row(Row(Row(1, 2, 3), Row(Row(4, "hello", "world"), Row(7, 8, 9)))) :: Nil,
      expectedSchema)
  }

  test("update_struct should handle column names with dots that are properly escaped using backticks") {
    val structLevel3 = spark.createDataFrame(sparkContext.parallelize(
      Seq(Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))))),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("important.metric", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))

    checkAnswer(
      structLevel3.withColumn("a", update_struct(
        "a.`important.metric`.a",
        $"a.`important.metric`.a".withField("b", lit(5)))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType)))),
          StructField("important.metric", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType, false),
              StructField("c", IntegerType)))),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType),
              StructField("b", IntegerType),
              StructField("c", IntegerType)))))))))))))
  }

  test("update_struct should throw exception if first or intermediate fields do not exist") {
    val func: String => Column = update_struct(_, lit("hello"))

    intercept[AnalysisException] {
      structLevel3.withColumn("a", func("xxx.b.a")).show(false)
    }.getMessage() should include("cannot resolve '`xxx`' given input columns: [a]")

    intercept[AnalysisException] {
      structLevel3.withColumn("a", func("a.xxx.a")).show(false)
    }.getMessage() should include("No such struct field xxx in a, b")
  }
}

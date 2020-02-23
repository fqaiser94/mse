package org.apache.spark.sql.catalyst.optimizer

import com.mse.column.methods._
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import org.scalatest.Matchers

class SimplifyStructExpressionsQueryTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private lazy val df = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))


  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.experimental.extraOptimizations = SimplifyStructExpressions.rules
  }

  /**
    * dropFields should work same as without optimization in [[com.mse.column.dropFieldsTest]]
    * this is mostly to check that [[org.apache.spark.sql.catalyst.optimizer.SimplifyDropFieldsDropFields]] is working as expected
    */

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

  test("drop multiple fields in struct with multiple dropFields calls") {
    checkAnswer(
      df.withColumn("a", $"a".dropFields("c").dropFields("a")),
      Row(Row(2)) :: Nil)
  }

  test("drop all fields in struct") {
    checkAnswer(
      df.withColumn("a", $"a".dropFields("c", "a", "b")),
      Row(Row()) :: Nil)
  }

  // TODO: more tests
  // TODO: all tests should verify schema

  test("pre-existing struct") {
    val result = df.withColumn("a", $"a".withField("d", lit(4)).withFieldRenamed("a", "x").dropFields("c")).cache
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    println(explain)

    explain should not include "add_fields"
    explain should not include "rename_fields"
    explain should not include "drop_fields"

    checkAnswer(
      result,
      Row(Row(1, 2, 4)) :: Nil)
  }

  test("pre-existing null struct") {
    val df = spark.createDataFrame(sparkContext.parallelize(
      Row(null) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))))))
    val result = df.withColumn("a", $"a".withField("d", lit(4)).withFieldRenamed("a", "x").dropFields("c")).cache
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    println(explain)

    explain should not include "add_fields"
    explain should not include "rename_fields"
    explain should not include "drop_fields"

    checkAnswer(
      result,
      Row(null) :: Nil)
  }

  test("new struct") {
    val df = spark.createDataFrame(sparkContext.parallelize(
      Row(1, 2, 3) :: Nil),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType))))

    val result = df.select(struct('a, 'b, 'c).withField("d", lit(4)).withFieldRenamed("a", "x").as("a"))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    println(explain)

    explain should not include "add_fields"
    explain should not include "rename_fields"
    explain should not include "drop_fields"

    checkAnswer(
      result,
      Row(Row(1, 2, 3, 4)) :: Nil)
  }
}

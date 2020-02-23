package org.apache.spark.sql.catalyst.optimizer

import com.mse.column.methods._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import org.scalatest.Matchers

class SimplifyStructBasedExpressionsTest extends QueryTest with SharedSparkSession with Matchers {

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
    spark.experimental.extraOptimizations = SimplifyStructBasedExpressions.rules
  }

  /**
    * dropFields should work same as without optimization in [[com.mse.column.dropFieldsTest]]
    * this is mostly to check that [[org.apache.spark.sql.catalyst.optimizer.SimplifySuccessiveDropFieldsExpressions]] is working as expected
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

}

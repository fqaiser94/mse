package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite
import com.mse.column.methods._

class SimplifyStructExpressionsTest extends FunSuite {

  private val spark = SparkSession.builder().appName("spark-test").master("local").getOrCreate()
  private val sparkContext = spark.sparkContext
  import spark.implicits._

  private lazy val df = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3, 4)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))

  def result = df.withColumn("a", $"a".withFieldRenamed("a", "x").withFieldRenamed("b", "y").withFieldRenamed("c", "z"))

  test("without rule") {
    result.explain
    // *(1) Project [rename_field(rename_field(rename_field(a#1, a, x), b, y), c, z) AS a#3]
  }

  test("with rule") {
    spark.experimental.extraOptimizations = Seq(SimplifyStructExpressions)
    result.explain
  }


}

package org.apache.spark.sql.catalyst.expressions

import com.mse.column.methods._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.FunSuite

class SimplifyStructExpressionsTest extends FunSuite {

  private val spark = SparkSession.builder().appName("spark-test").master("local").getOrCreate()
  spark.experimental.extraOptimizations = Seq(SimplifyStructExpressions)
  private val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  private val df: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3, 4)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))

  test("rename rule") {
    val result: DataFrame = df.withColumn("a", $"a".withFieldRenamed("a", "x").withFieldRenamed("b", "y").withFieldRenamed("c", "z"))

    result.explain
    result.printSchema()
    result.show(false)
  }

  test("drop rule") {
    val result: DataFrame = df.withColumn("a", $"a".dropFields("a").dropFields("b"))

    result.explain
    result.printSchema()
    result.show(false)
  }

  test("add rule") {
    val result: DataFrame = df.withColumn("a", $"a".withField("a", lit(4)).withField("b", lit(5)).withField("c", lit(6)))

    result.explain
    result.printSchema()
    result.show(false)
  }

}

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


class CollapseSuccessiveDropFieldsExpressionsTest extends PlanTest with ExpressionEvalHelper {

  private val spark = SparkSession.builder().appName("spark-test").master("local").getOrCreate()
  spark.experimental.extraOptimizations = Seq(CollapseSuccessiveDropFieldsExpressions)
  private val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  //  private val df: DataFrame = spark.createDataFrame(sparkContext.parallelize(
  //    Row(Row(1, 2, 3, 4)) :: Nil),
  //    StructType(Seq(
  //      StructField("a", StructType(Seq(
  //        StructField("a", IntegerType),
  //        StructField("b", IntegerType),
  //        StructField("c", IntegerType)))))))
  //
  //  test("drop rule") {
  //    val result: DataFrame = df.withColumn("a", $"a".dropFields("a").dropFields("b"))
  //
  //    result.explain
  //    result.printSchema()
  //    result.show(false)
  //  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      "CollapseDropFieldsExpressionsTest",
      FixedPoint(50),
      CollapseSuccessiveDropFieldsExpressions) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)
    comparePlans(actual, correctAnswer)
  }

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))
    val fieldValues = Array(1, 2, 3)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should collapse successive DropFields into one") {
    assertEquivalent(
      DropFields(DropFields(inputStruct, "b"), "c"),
      DropFields(inputStruct, "b", "c"))

    assertEquivalent(
      DropFields(DropFields(inputStruct, "a", "b"), "c"),
      DropFields(inputStruct, "a", "b", "c"))
  }
}

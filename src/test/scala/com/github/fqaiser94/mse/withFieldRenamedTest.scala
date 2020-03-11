package com.github.fqaiser94.mse

import methods._
import org.apache.spark.sql.catalyst.optimizer.SimplifyStructExpressions
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

trait withFieldRenamedTests extends QueryTester {

  import testImplicits._

  private val structSchema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", IntegerType),
    StructField("c", IntegerType),
    StructField("c", IntegerType)))

  lazy val structDf: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3, 4)) :: Nil),
    StructType(Seq(
      StructField("a", structSchema))))

  lazy val nullStructDf: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(null) :: Nil),
    StructType(Seq(
      StructField("a", structSchema))))

  test("throw error if withField is called on a column that is not struct dataType") {
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".withFieldRenamed("a", "z"))
    }.getMessage should include("struct should be struct data type. struct is integer")
  }

  test("throw error if null existingFieldName supplied") {
    intercept[AnalysisException] {
      structDf.withColumn("a", $"a".withFieldRenamed(null, "z"))
    }.getMessage should include("existingFieldName cannot be null")
  }

  test("throw error if null newFieldName supplied") {
    intercept[AnalysisException] {
      structDf.withColumn("a", $"a".withFieldRenamed("a", null))
    }.getMessage should include("newFieldName cannot be null")
  }

  test("rename field in struct") {
    val expectedValue = Row(Row(1, 2, 3, 4)) :: Nil
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("y", IntegerType),
        StructField("c", IntegerType),
        StructField("c", IntegerType))))))

    checkAnswer(
      structDf.withColumn("a", 'a.withFieldRenamed("b", "y")),
      expectedValue,
      expectedSchema)

    checkAnswer(
      structDf.withColumn("a", $"a".withFieldRenamed("b", "y")),
      expectedValue,
      expectedSchema)
  }

  test("rename multiple fields in struct") {
    checkAnswer(
      structDf.withColumn("a", $"a".withFieldRenamed("c", "x")),
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
      structDf.withColumn("a", $"a".withFieldRenamed("d", "x")),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
          StructField("c", IntegerType)))))))
  }

  test("return null for if struct is null") {
    checkAnswer(
      nullStructDf.withColumn("a", $"a".withFieldRenamed("b", "y")),
      Row(null) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("y", IntegerType),
          StructField("c", IntegerType),
          StructField("c", IntegerType)))))))

  }
}

class withFieldRenamedTest extends withFieldRenamedTests {

  import testImplicits._

  test("multiple rename_fields calls will stay as multiple rename_fields calls") {
    val result = structDf.withColumn("a", $"a".withFieldRenamed("a", "x").withFieldRenamed("b", "y"))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "rename_fields".r.findAllMatchIn(explain).size shouldEqual 2
  }
}

class withFieldRenamedTestWithOptimization extends withFieldRenamedTests {

  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.experimental.extraOptimizations = SimplifyStructExpressions.rules
  }

  test("multiple rename_fields calls should be collapsed in a single rename_fields call") {
    val result = structDf.withColumn("a", $"a".withFieldRenamed("a", "x").withFieldRenamed("b", "y"))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "rename_fields".r.findAllMatchIn(explain).size shouldEqual 1
  }
}

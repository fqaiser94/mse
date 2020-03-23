package com.github.fqaiser94.mse

import com.github.fqaiser94.mse.methods._
import org.apache.spark.sql.catalyst.optimizer.SimplifyStructExpressions
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

trait dropFieldsTests extends QueryTester {

  import testImplicits._

  private val structSchema: StructType = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", IntegerType),
    StructField("c", IntegerType)))

  lazy val structDf: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3)) :: Nil),
    StructType(Seq(
      StructField("a", structSchema))))

  lazy val nullStructDf: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(null) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))

  test("throw error if withField is called on a column that is not struct dataType") {
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".dropFields("a"))
    }.getMessage should include("Only struct is allowed to appear at first position, got: integer")
  }

  test("throw error if null fieldName supplied") {
    intercept[AnalysisException] {
      structDf.withColumn("a", $"a".dropFields(null))
    }.getMessage should include("Only non-null foldable string expressions are allowed after first position.")
  }

  test("drop field in struct") {
    val expectedValue = Row(Row(1, 3)) :: Nil
    val expectedSchema = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType))))))

    checkAnswer(
      structDf.withColumn("a", 'a.dropFields("b")),
      expectedValue,
      expectedSchema)

    checkAnswer(
      structDf.withColumn("a", $"a".dropFields("b")),
      expectedValue,
      expectedSchema)
  }

  test("drop multiple fields in struct") {
    checkAnswer(
      structDf.withColumn("a", $"a".dropFields("c", "a")),
      Row(Row(2)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("b", IntegerType)))))))
  }

  test("drop all fields in struct") {
    checkAnswer(
      structDf.withColumn("a", $"a".dropFields("c", "a", "b")),
      Row(Row()) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq.empty)))))
  }

  test("drop multiple fields in struct with multiple dropFields calls") {
    checkAnswer(
      structDf.withColumn("a", $"a".dropFields("c").dropFields("a")),
      Row(Row(2)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("b", IntegerType)))))))
  }

  test("return null for if struct is null") {
    checkAnswer(
      nullStructDf.withColumn("a", $"a".dropFields("c")),
      Row(null) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType)))))))
  }

}

class dropFieldsTest extends dropFieldsTests {

  import testImplicits._

  test("multiple dropFields calls will stay as multiple dropFields calls") {
    val result = structDf.withColumn("a", $"a".dropFields("c").dropFields("a"))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "drop_fields".r.findAllMatchIn(explain).size shouldEqual 2
  }
}

class dropFieldsTestWithOptimization extends dropFieldsTests {

  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.experimental.extraOptimizations = SimplifyStructExpressions.rules
  }

  test("multiple dropFields calls should be collapsed in a single dropFields call") {
    val result = structDf.withColumn("a", $"a".dropFields("c").dropFields("a"))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "drop_fields".r.findAllMatchIn(explain).size shouldEqual 1
  }
}

package com.github.fqaiser94.mse

import methods._
import org.apache.spark.sql.Column
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
    }.getMessage should include("Only struct is allowed to appear at first position, got: integer.")
  }

  test("throw error if null existingFieldName supplied") {
    intercept[AnalysisException] {
      structDf.withColumn("a", $"a".withFieldRenamed(null, "z"))
    }.getMessage should include("Only non-null foldable string expressions are allowed to appear after first position.")
  }

  test("throw error if null newFieldName supplied") {
    intercept[AnalysisException] {
      structDf.withColumn("a", $"a".withFieldRenamed("a", null))
    }.getMessage should include("Only non-null foldable string expressions are allowed to appear after first position.")
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

  test("rename by function correctly") {
    import org.apache.spark.sql.functions.concat
    import org.apache.spark.sql.functions.lit
    import org.apache.spark.sql.functions.when
    val renameFn: Column => Column = { col =>
      // if the field is not named "a", then suffix with "baz", otherwise suffix with "bar"
      concat(col, when(col =!= "a", lit("bar")).otherwise(lit("baz")))
    }

    checkAnswer(
      structDf.withColumn("a", $"a".withFieldRenamedByFn(renameFn)),
      Row(Row(1, 2, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("abaz", IntegerType),
          StructField("bbar", IntegerType),
          StructField("cbar", IntegerType),
          StructField("cbar", IntegerType)))))))
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

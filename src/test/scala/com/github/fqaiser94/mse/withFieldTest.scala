package com.github.fqaiser94.mse

import methods._
import org.apache.spark.sql.catalyst.optimizer.SimplifyStructExpressions
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

trait withFieldTests extends QueryTester {

  import testImplicits._

  private lazy val structLevel1Schema =
    StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))

  private lazy val structLevel2Schema =
    StructType(Seq(
      StructField("a", structLevel1Schema)))

  private lazy val structLevel3Schema =
    StructType(Seq(
      StructField("a", structLevel1Schema),
      StructField("b", StructType(Seq(
        StructField("a", structLevel1Schema),
        StructField("b", structLevel1Schema))))))

  private lazy val structLevel1WithNewFieldSchema =
    structLevel1Schema.add("d", IntegerType, nullable = false)

  private lazy val structLevel2WithNewFieldSchema =
    StructType(Seq(
      StructField("a", structLevel1WithNewFieldSchema)))

  private lazy val structLevel3WithNewFieldSchema =
    StructType(Seq(
      StructField("a", structLevel1Schema),
      StructField("b", StructType(Seq(
        StructField("a", structLevel1WithNewFieldSchema),
        StructField("b", structLevel1Schema))))))

  lazy val structLevel1: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 1, 1)) :: Nil),
    StructType(Seq(StructField("a", structLevel1Schema))))

  lazy val nullStructLevel1: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(null) :: Nil),
    StructType(Seq(StructField("a", structLevel1Schema))))

  lazy val structLevel2: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(Row(1, 1, 1))) :: Nil),
    StructType(Seq(StructField("a", structLevel2Schema))))

  lazy val nullStructLevel2: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Row(null) :: Nil),
    StructType(Seq(StructField("a", structLevel2Schema))))

  lazy val structLevel3: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Seq(Row(Row(Row(1, 2, 3), Row(Row(4, null, 6), Row(7, 8, 9)))))),
    StructType(Seq(StructField("a", structLevel3Schema))))

  lazy val nullStructLevel3: DataFrame = spark.createDataFrame(sparkContext.parallelize(
    Seq(Row(null))),
    StructType(Seq(StructField("a", structLevel3Schema))))

  test("throw error if withField is called on a column that is not struct dataType") {
    intercept[AnalysisException] {
      testData.withColumn("key", $"key".withField("a", lit(2)))
    }.getMessage should include("struct should be struct data type. struct is integer")
  }

  test("throw error if null fieldName supplied") {
    intercept[AnalysisException] {
      structLevel1.withColumn("a", $"a".withField(null, lit(2)))
    }.getMessage should include("fieldNames cannot contain null")
  }

  test("add new field to struct") {
    val expectedValue = Row(Row(1, 1, 1, 2)) :: Nil
    val expectedSchema = StructType(Seq(StructField("a", structLevel1WithNewFieldSchema)))

    checkAnswer(
      structLevel1.withColumn("a", 'a.withField("d", lit(2))),
      expectedValue,
      expectedSchema)

    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(2))),
      expectedValue,
      expectedSchema)
  }

  test("add multiple new fields to struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(2)).withField("e", lit(3))),
      Row(Row(1, 1, 1, 2, 3)) :: Nil,
      StructType(Seq(StructField("a", structLevel1Schema.add("d", IntegerType, nullable = false).add("e", IntegerType, nullable = false)))))
  }

  test("replace field in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(2))),
      Row(Row(1, 2, 1)) :: Nil,
      StructType(Seq(StructField("a", StructType(structLevel1Schema.updated(1, StructField("b", IntegerType, nullable = false)))))))
  }

  test("replace multiple fields in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("a", lit(2)).withField("b", lit(2))),
      Row(Row(2, 2, 1)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(structLevel1Schema
          .updated(0, StructField("a", IntegerType, nullable = false))
          .updated(1, StructField("b", IntegerType, nullable = false)))))))
  }

  test("add and replace fields in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(2)).withField("d", lit(2))),
      Row(Row(1, 2, 1, 2)) :: Nil,
      StructType(Seq(StructField("a", StructType(structLevel1WithNewFieldSchema.updated(1, StructField("b", IntegerType, nullable = false)))))))
  }

  test("add new field to nested struct") {
    checkAnswer(
      structLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "d", lit(2)))),
      Row(Row(Row(1, 1, 1, 2))) :: Nil,
      StructType(Seq(StructField("a", structLevel2WithNewFieldSchema))))
  }

  test("replace field in nested struct") {
    checkAnswer(
      structLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "b", lit(2)))),
      Row(Row(Row(1, 2, 1))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(structLevel1Schema.updated(1, StructField("b", IntegerType, nullable = false))))))))))
  }

  test("add field to deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "d", lit(0))))),
      Row(Row(Row(1, 2, 3), Row(Row(4, null, 6, 0), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(StructField("a", structLevel3WithNewFieldSchema))))
  }

  test("replace field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "b", lit(5))))),
      Row(Row(Row(1, 2, 3), Row(Row(4, 5, 6), Row(7, 8, 9)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", structLevel1Schema),
          StructField("b", StructType(Seq(
            StructField("a", StructType(
              structLevel1Schema.updated(1, StructField("b", IntegerType, nullable = false)))),
            StructField("b", structLevel1Schema))))))))))
  }

  test("return null if struct is null") {
    checkAnswer(
      nullStructLevel1.withColumn("a", $"a".withField("d", lit(2))),
      Row(null) :: Nil,
      StructType(Seq(StructField("a", structLevel1WithNewFieldSchema))))

    checkAnswer(
      nullStructLevel2.withColumn("a", $"a".withField(
        "a", $"a.a".withField(
          "d", lit(2)))),
      Row(null) :: Nil,
      StructType(Seq(StructField("a", structLevel2WithNewFieldSchema))))

    checkAnswer(
      nullStructLevel3.withColumn("a", $"a".withField(
        "b", $"a.b".withField(
          "a", $"a.b.a".withField(
            "d", lit(0))))),
      Row(null) :: Nil,
      StructType(Seq(StructField("a", structLevel3WithNewFieldSchema))))
  }
}

class withFieldTest extends withFieldTests {

  import testImplicits._

  test("multiple add_fields calls will stay as multiple add_fields calls") {
    val result = structLevel1.withColumn("a", $"a".withField("b", lit(2)).withField("d", lit(2)))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "add_fields".r.findAllMatchIn(explain).size shouldEqual 2
  }

  test("multiple nested add_fields calls will stay as multiple nested add_fields calls") {
    val result = structLevel2.withColumn("a", $"a".withField("a", $"a.a".withField("d", lit(2))).withField("b", lit(2)))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "add_fields".r.findAllMatchIn(explain).size shouldEqual 3
  }
}

class withFieldTestWithOptimization extends withFieldTests {

  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.experimental.extraOptimizations = SimplifyStructExpressions.rules
  }

  test("multiple add_fields calls should be collapsed in a single add_fields call") {
    val result = structLevel1.withColumn("a", $"a".withField("b", lit(2)).withField("d", lit(2)))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "add_fields".r.findAllMatchIn(explain).size shouldEqual 1
  }

  test("multiple nested add_fields calls should be collapsed in a single add_fields call") {
    val result = structLevel2.withColumn("a", $"a".withField("a", $"a.a".withField("d", lit(2))).withField("b", lit(2)))
    val explain = ExplainCommand(result.queryExecution.logical).run(spark).head.getAs[String](0)

    "add_fields".r.findAllMatchIn(explain).size shouldEqual 2
  }
}

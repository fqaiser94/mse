package org.apache.spark.sql.catalyst.optimizer

import com.mse.column.methods._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import org.scalatest.Matchers

// TODO: Delete
class TempTest extends QueryTest with SharedSparkSession with Matchers {

  import testImplicits._

  private lazy val structLevel1 = spark.createDataFrame(sparkContext.parallelize(
    Row(Row(1, 2, 3)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))))))

  test("add new field to struct") {
    spark.sparkContext.setLogLevel("ERROR")

    def runTest = {
      val result = structLevel1.withColumn("a", $"a".dropFields("c").withField("d", lit(4)))
      result.explain
      result.printSchema()
      result.show(false)

      checkAnswer(
        result,
        Row(Row(1, 2, 4)) :: Nil)
    }

    runTest

    spark.experimental.extraOptimizations = Seq(SimplifySuccessiveAddFieldsCreateNamedStructExpressions)
    runTest
  }

}

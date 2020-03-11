package com.github.fqaiser94.mse

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.scalatest.Matchers

trait QueryTester extends QueryTest with SharedSparkSession with Matchers {

  def checkAnswer(df: => DataFrame,
                  expectedAnswer: Seq[Row],
                  expectedSchema: StructType): Unit = {

    assert(df.schema == expectedSchema)
    checkAnswer(df, expectedAnswer)
  }

}

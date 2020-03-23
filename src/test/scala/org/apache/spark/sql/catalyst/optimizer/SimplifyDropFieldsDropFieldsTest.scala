package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{DropFields, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifyDropFieldsDropFieldsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifyDropFieldsDropFields) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  private val (inputStruct, nullInputStruct) = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType),
      StructField("d", IntegerType),
      StructField("e", IntegerType),
      StructField("f", IntegerType)))
    val fieldValues = Array(1, 2, 3, 4, 5, 6)

    Tuple2(Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema))
  }

  test("should collapse successive DropFields call into a single DropFields call") {
    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct :: Literal("b") :: Nil) :: Literal("c") :: Nil),
      DropFields(inputStruct :: Literal("b") :: Literal("c") :: Nil),
      create_row(1, 4, 5, 6),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(nullInputStruct :: Literal("b") :: Nil) :: Literal("c") :: Nil),
      DropFields(nullInputStruct :: Literal("b") :: Literal("c") :: Nil),
      null,
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct :: Literal("a") :: Literal("b") :: Nil) :: Literal("c") :: Nil),
      DropFields(inputStruct :: Literal("a") :: Literal("b") :: Literal("c") :: Nil),
      create_row(4, 5, 6),
      StructType(Seq(
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct :: Literal("a") :: Nil) :: Literal("b") :: Literal("c") :: Nil),
      DropFields(inputStruct :: Literal("a") :: Literal("b") :: Literal("c") :: Nil),
      create_row(4, 5, 6),
      StructType(Seq(
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct :: Literal("a") :: Literal("b") :: Nil) :: Literal("c") :: Literal("d") :: Nil),
      DropFields(inputStruct :: Literal("a") :: Literal("b") :: Literal("c") :: Literal("d") :: Nil),
      create_row(5, 6),
      StructType(Seq(
        StructField("e", IntegerType),
        StructField("f", IntegerType))))
  }
}

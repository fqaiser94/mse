package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifyRenameFieldsRenameFieldsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifyRenameFieldsRenameFields) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  private val (inputStruct, nullInputStruct) = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))
    val fieldValues = Array(1, 2, 3)

    Tuple2(
      Literal.create(create_row(fieldValues: _*), schema),
      Literal.create(null, schema))
  }

  test("should simplify successive RenameFields call into a single RenameFields call") {
    val expectedEvaluationResult = create_row(1, 2, 3)

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(inputStruct :: Literal("a") :: Literal("x") :: Nil) :: Literal("b") :: Literal("y") :: Nil),
      RenameFields(inputStruct :: Literal("a") :: Literal("x") :: Literal("b") :: Literal("y") :: Nil),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("c", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(nullInputStruct :: Literal("a") :: Literal("x") :: Nil) :: Literal("b") :: Literal("y") :: Nil),
      RenameFields(nullInputStruct :: Literal("a") :: Literal("x") :: Literal("b") :: Literal("y") :: Nil),
      null,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("c", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(inputStruct :: Literal("a") :: Literal("x") :: Literal("b") :: Literal("y") :: Nil) :: Literal("c") :: Literal("z") :: Nil),
      RenameFields(inputStruct :: Literal("a") :: Literal("x") :: Literal("b") :: Literal("y") :: Literal("c") :: Literal("z") :: Nil),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      RenameFields(RenameFields(inputStruct :: Literal("a") :: Literal("x") :: Nil) :: Literal("b") :: Literal("y") :: Literal("c") :: Literal("z") :: Nil),
      RenameFields(inputStruct :: Literal("a") :: Literal("x") :: Literal("b") :: Literal("y") :: Literal("c") :: Literal("z") :: Nil),
      expectedEvaluationResult,
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))
  }
}

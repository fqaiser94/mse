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

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType),
      StructField("d", IntegerType),
      StructField("e", IntegerType),
      StructField("f", IntegerType)))
    val fieldValues = Array(1, 2, 3, 4, 5, 6)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should collapse successive DropFields call into a single DropFields call") {
    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "b"), "c"),
      DropFields(inputStruct, "b", "c"),
      create_row(1, 4, 5, 6),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "a", "b"), "c"),
      DropFields(inputStruct, "a", "b", "c"),
      create_row(4, 5, 6),
      StructType(Seq(
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "a"), "b", "c"),
      DropFields(inputStruct, "a", "b", "c"),
      create_row(4, 5, 6),
      StructType(Seq(
        StructField("d", IntegerType),
        StructField("e", IntegerType),
        StructField("f", IntegerType))))

    assertEquivalentPlanAndEvaluation(
      DropFields(DropFields(inputStruct, "a", "b"), "c", "d"),
      DropFields(inputStruct, "a", "b", "c", "d"),
      create_row(5, 6),
      StructType(Seq(
        StructField("e", IntegerType),
        StructField("f", IntegerType))))
  }

  // TODO: test for null struct
}

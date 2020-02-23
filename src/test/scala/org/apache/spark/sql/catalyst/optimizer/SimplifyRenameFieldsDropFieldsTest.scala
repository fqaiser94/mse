package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, DropFields, GetStructField, Literal, RenameFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifyRenameFieldsDropFieldsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifyRenameFieldsDropFields) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false),
      StructField("c", IntegerType, nullable = false)))
    val fieldValues = Array(1, 2, 3)
    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should correctly simplify RenameFields and DropFields into CreateNamedStruct") {
    assertEquivalentPlanAndEvaluation(
      RenameFields(DropFields(inputStruct, "b"), "c", "b"),
      CreateNamedStruct(Seq("a", GetStructField(inputStruct, 0), "b", GetStructField(inputStruct, 2))),
      create_row(1, 3),
      StructType(Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", IntegerType, nullable = false))))
  }

  test("should correctly simplify DropFields and RenameFields into CreateNamedStruct") {
    assertEquivalentPlanAndEvaluation(
      DropFields(RenameFields(inputStruct, "b", "z"), "c"),
      CreateNamedStruct(Seq("a", GetStructField(inputStruct, 0), "z", GetStructField(inputStruct, 1))),
      create_row(1, 2),
      StructType(Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("z", IntegerType, nullable = false))))
  }

  test("should not rename any fields that have already been dropped in CreateNamedStruct") {
    assertEquivalentPlanAndEvaluation(
      RenameFields(DropFields(inputStruct, "c"), "c", "z"),
      CreateNamedStruct(Seq("a", GetStructField(inputStruct, 0), "b", GetStructField(inputStruct, 1))),
      create_row(1, 2),
      StructType(Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", IntegerType, nullable = false))))
  }

  test("should not drop any fields that have already been renamed in CreateNamedStruct") {
    assertEquivalentPlanAndEvaluation(
      DropFields(RenameFields(inputStruct, "c", "z"), "c"),
      CreateNamedStruct(Seq("a", GetStructField(inputStruct, 0), "b", GetStructField(inputStruct, 1), "z", GetStructField(inputStruct, 2))),
      create_row(1, 2, 3),
      StructType(Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", IntegerType, nullable = false),
        StructField("z", IntegerType, nullable = false))))
  }

  // TODO: test for null struct
}

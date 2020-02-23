package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, CreateNamedStruct, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class ReplaceAddFieldsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      ReplaceAddFields) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  private val inputStruct = {
    val schema = StructType(Seq(StructField("a", IntegerType)))
    val fieldValues = Array(1)

    Literal.create(create_row(fieldValues: _*), schema)
  }

  private val inputStructNullable = {
    val schema = StructType(Seq(StructField("a", IntegerType)))
    Literal.create(null, schema)
  }

  // TODO: complete writing tests

  test("struct") {
    val nameA = "b"
    val exprA = Literal.create(2, IntegerType)

    assertEquivalentPlanAndEvaluation(
      AddFields(inputStruct, nameA, exprA),
      CreateNamedStruct(Seq(Literal("a"), GetStructField(inputStruct, 0), Literal("b"), Literal(2, IntegerType))),
      create_row(1, exprA.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable))))
  }

  test("nullable struct") {
    val nameA = "b"
    val exprA = Literal.create(2, IntegerType)

    assertEquivalentPlanAndEvaluation(
      AddFields(inputStructNullable, nameA, exprA),
      AddFields(inputStructNullable, nameA, exprA),
      null,
      StructType(Seq(
        StructField("a", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable))))
  }

}
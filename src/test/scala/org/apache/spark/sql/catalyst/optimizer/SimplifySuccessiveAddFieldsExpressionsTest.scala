package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifySuccessiveAddFieldsExpressionsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifySuccessiveAddFieldsExpressions) :: Nil
  }

  override val Optimizer: RuleExecutor[LogicalPlan] = Optimize

  private val inputStruct = {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType)))
    val fieldValues = Array(1, 2, 3)

    Literal.create(create_row(fieldValues: _*), schema)
  }

  test("should do nothing to a single AddFields call") {
    val nameA = "d"
    val exprA = Literal.create(4, IntegerType)

    assertEquivalentPlanAndEvaluation(
      AddFields(inputStruct, nameA, exprA),
      AddFields(inputStruct, nameA, exprA),
      create_row(1, 2, 3, exprA.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable))))
  }

  test("should collapse successive AddFields call into a single AddFields call") {
    val nameA = "c"
    val exprA = Literal.create(0, IntegerType)

    val nameB = "d"
    val exprB = Literal.create(4, IntegerType)

    val nameC = "e"
    val exprC = Literal.create(5, IntegerType)

    val nameD = "f"
    val exprD = Literal.create(6, IntegerType)

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct, nameA, exprA), nameB, exprB),
      AddFields(inputStruct, Seq(nameA, nameB), Seq(exprA, exprB)),
      create_row(1, 2, exprA.value, exprB.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct, Seq(nameA, nameB), Seq(exprA, exprB)), nameC, exprC),
      AddFields(inputStruct, Seq(nameA, nameB, nameC), Seq(exprA, exprB, exprC)),
      create_row(1, 2, exprA.value, exprB.value, exprC.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable),
        StructField(nameC, exprC.dataType, exprC.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct, nameA, exprA), Seq(nameB, nameC), Seq(exprB, exprC)),
      AddFields(inputStruct, Seq(nameA, nameB, nameC), Seq(exprA, exprB, exprC)),
      create_row(1, 2, exprA.value, exprB.value, exprC.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable),
        StructField(nameC, exprC.dataType, exprC.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct, Seq(nameA, nameB), Seq(exprA, exprB)), Seq(nameC, nameD), Seq(exprC, exprD)),
      AddFields(inputStruct, Seq(nameA, nameB, nameC, nameD), Seq(exprA, exprB, exprC, exprD)),
      create_row(1, 2, exprA.value, exprB.value, exprC.value, exprD.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable),
        StructField(nameC, exprC.dataType, exprC.nullable),
        StructField(nameD, exprD.dataType, exprD.nullable))))
  }

  // TODO: test for null struct
}

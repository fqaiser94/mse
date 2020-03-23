package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{AddFields, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class SimplifyAddFieldsAddFieldsTest extends OptimizerTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Optimize.Batch] = Batch(
      this.getClass.getSimpleName,
      FixedPoint(50),
      SimplifyAddFieldsAddFields) :: Nil
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

  test("should do nothing to a single AddFields call") {
    val nameA = "d"
    val exprA = Literal.create(4, IntegerType)

    assertEquivalentPlanAndEvaluation(
      AddFields(inputStruct :: Literal(nameA) :: exprA :: Nil),
      AddFields(inputStruct :: Literal(nameA) :: exprA :: Nil),
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
      AddFields(AddFields(inputStruct :: Literal(nameA) :: exprA :: Nil) :: Literal(nameB) :: exprB :: Nil),
      AddFields(inputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Nil),
      create_row(1, 2, exprA.value, exprB.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(nullInputStruct :: Literal(nameA) :: exprA :: Nil) :: Literal(nameB) :: exprB :: Nil),
      AddFields(nullInputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Nil),
      null,
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Nil) :: Literal(nameC) :: exprC :: Nil),
      AddFields(inputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Literal(nameC) :: exprC :: Nil),
      create_row(1, 2, exprA.value, exprB.value, exprC.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable),
        StructField(nameC, exprC.dataType, exprC.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct :: Literal(nameA) :: exprA :: Nil) :: Literal(nameB) :: exprB :: Literal(nameC) :: exprC :: Nil),
      AddFields(inputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Literal(nameC) :: exprC :: Nil),
      create_row(1, 2, exprA.value, exprB.value, exprC.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable),
        StructField(nameC, exprC.dataType, exprC.nullable))))

    assertEquivalentPlanAndEvaluation(
      AddFields(AddFields(inputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Nil) :: Literal(nameC) :: exprC :: Literal(nameD) :: exprD :: Nil),
      AddFields(inputStruct :: Literal(nameA) :: exprA :: Literal(nameB) :: exprB :: Literal(nameC) :: exprC :: Literal(nameD) :: exprD :: Nil),
      create_row(1, 2, exprA.value, exprB.value, exprC.value, exprD.value),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField(nameA, exprA.dataType, exprA.nullable),
        StructField(nameB, exprB.dataType, exprB.nullable),
        StructField(nameC, exprC.dataType, exprC.nullable),
        StructField(nameD, exprD.dataType, exprD.nullable))))
  }
}

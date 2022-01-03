package com.pancakedb.client

import com.pancakedb.client.PrimitiveHandlers.{BoolHandler, StringHandler}
import com.pancakedb.idl.{FieldValue, RepeatedFieldValue}

class AtomNesterTest extends TestBase {
  val boolColumn: RepLevelsColumn[Boolean] = RepLevelsColumn(
    Array(true, false, false, true),
    Array(0, 1, 1, 1, 0, 1),
    0,
    6,
  )
  val expectedBoolFvs: Array[FieldValue] = Array(
    FieldValue.newBuilder().build(),
    FieldValue.newBuilder().setBoolVal(true).build(),
    FieldValue.newBuilder().setBoolVal(false).build(),
    FieldValue.newBuilder().setBoolVal(false).build(),
  )

  "nextFieldValue" should "iterate one at a time" in {
    val nester = AtomNester(BoolHandler, boolColumn)
    assertResult(expectedBoolFvs(0))(nester.nextFieldValue())
    assertResult(expectedBoolFvs(1))(nester.nextFieldValue())
    assertResult(expectedBoolFvs(2))(nester.nextFieldValue())
    assertResult(expectedBoolFvs(3))(nester.nextFieldValue())
  }

  "computeFieldValues" should "return all the field values" in {
    val nester = AtomNester(BoolHandler, boolColumn)
    assertResult(expectedBoolFvs)(nester.computeFieldValues(4))
  }

  "computeFieldValues" should "work for complicated columns" in {
    val stringsColumn = RepLevelsColumn[Byte](
      Array(97, 98, 99, 100).map(_.toByte),
      Array(2, 0, 1, 3, 3, 3, 2, 3, 2, 1),
      1,
      4,
    )
    val nester = AtomNester(StringHandler, stringsColumn)
    val expected = Array(
      FieldValue.newBuilder()
        .setListVal(RepeatedFieldValue.newBuilder()
          .addVals(
            FieldValue.newBuilder().setStringVal("")
          )
        ).build(),
      FieldValue.newBuilder().build(),
      FieldValue.newBuilder()
        .setListVal(RepeatedFieldValue.newBuilder()).build(),
      FieldValue.newBuilder()
        .setListVal(RepeatedFieldValue.newBuilder()
          .addVals(FieldValue.newBuilder().setStringVal("abc"))
          .addVals(FieldValue.newBuilder().setStringVal("d"))
        ).build(),
    )
    val result = nester.computeFieldValues(4)
    assertResult(expected)(result)
  }
}

package com.pancakedb.client

import com.pancakedb.client.NativeCore.{BooleanColumn, StringOrBytesColumn}
import com.pancakedb.client.PrimitiveHandlers.{BoolHandler, StringHandler}

class PrimitiveHandlerTest extends TestBase {
  def assertRepLevelsEq[A](expected: RepLevelsColumn[A], result: RepLevelsColumn[A]): Unit = {
    assertResult(expected.atoms)(result.atoms)
    assertResult(expected.repLevels)(result.repLevels)
    assertResult(expected.nRows)(result.nRows)
    assertResult(expected.nestingDepth)(result.nestingDepth)
  }

  "filterToRepLevelsColumns" should "filter atoms and rep levels for atomic primitives" in {
    val nativeCol = BooleanColumn(
      Array(false, false, true, true),
      Array(2, 1, 0, 2, 2, 1, 2, 1, 0),
    )
    val result = BoolHandler.filterToRepLevelsColumn(
      nativeCol,
      1,
      4,
      implicitNullsCount = 2,
      deletions = Array(true, false, false, true, true), // first 2 apply to implicit nulls, last 3 explicit
    )
    val expected  = RepLevelsColumn[Boolean](
      Array(false, true),
      Array(0, 2, 1, 2, 1, 0),
      1,
      4,
    )
    assertRepLevelsEq(expected, result)
  }

  "filterToRepLevelsColumns" should "filter atoms and rep levels for non-atomic primitives" in {
    val nativeCol = StringOrBytesColumn(
      Array(97, 98, 99, 100),
      Array(2, 1, 0, 2, 2, 1, 2, 1, 0),
    )
    val result = StringHandler.filterToRepLevelsColumn(
      nativeCol,
      0,
      3,
      implicitNullsCount = 0,
      deletions = Array(false, true, true, false, false),
    )
    val expected  = RepLevelsColumn[Byte](
      Array(97, 100),
      Array(2, 1, 2, 1, 0),
      0,
      3,
    )
    assertRepLevelsEq(expected, result)
  }
}

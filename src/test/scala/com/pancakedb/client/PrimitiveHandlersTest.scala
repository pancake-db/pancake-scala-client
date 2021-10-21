package com.pancakedb.client

import com.pancakedb.client.NativeCore.BooleanColumn
import com.pancakedb.client.PrimitiveHandlers.BoolHandler

class PrimitiveHandlersTest extends TestBase {
  def assertColumnsEqual[A](expected: NativeRepLevelsColumn[A], result: NativeRepLevelsColumn[A]): Unit = {
    assertResult(expected.atoms)(result.atoms)
    assertResult(expected.repLevels)(result.repLevels)
  }

  "BoolHandler" should "call into rust and limit" in {
    val limit = 3
    val result = BoolHandler.decodeNativeRepLevelsColumn(
      0,
      Array(),
      Array(0, 0, 1, 0),
      limit,
      "q_compress"
    )
    val expected = BooleanColumn(
      Array(false, false, true),
      Array(1, 1, 1),
    )
    assertColumnsEqual(expected, result)
  }

  "BoolHandler" should "not error when limit is too high" in {
    val limit = 5
    val result = BoolHandler.decodeNativeRepLevelsColumn(
      0,
      Array(),
      Array(0, 0, 1, 0),
      limit,
      "q_compress"
    )
    val expected = BooleanColumn(
      Array(false, false, true, false),
      Array(1, 1, 1, 1),
    )
    assertColumnsEqual(expected, result)
  }
}

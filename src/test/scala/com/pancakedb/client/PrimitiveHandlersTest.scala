package com.pancakedb.client

import com.pancakedb.client.NativeCore.BooleanColumn
import com.pancakedb.client.PrimitiveHandlers.BoolHandler

class PrimitiveHandlersTest extends TestBase {
  val NullByte: Byte = 253.toByte

  def assertColumnsEqual[A](expected: NativeRepLevelsColumn[A], result: NativeRepLevelsColumn[A]): Unit = {
    assertResult(expected.atoms)(result.atoms)
    assertResult(expected.repLevels)(result.repLevels)
  }

  "BoolHandler" should "call into rust" in {
    val result = BoolHandler.decodeNativeRepLevelsColumn(
      0,
      Array(),
      Array(0, 0, 1, NullByte),
      "q_compress"
    )
    val expected = BooleanColumn(
      Array(false, false, true),
      Array(1, 1, 1, 0),
    )
    assertColumnsEqual(expected, result)
  }
}

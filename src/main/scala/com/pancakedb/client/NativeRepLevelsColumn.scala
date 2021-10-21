package com.pancakedb.client

private[client] trait NativeRepLevelsColumn[A] {
  val atoms: Array[A]
  val repLevels: Array[Byte]
}

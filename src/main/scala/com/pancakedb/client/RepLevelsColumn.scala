package com.pancakedb.client

case class RepLevelsColumn[A](
  atoms: Array[A],
  repLevels: Array[Byte],
  nestingDepth: Int,
  nRows: Int,
) extends NativeRepLevelsColumn[A]

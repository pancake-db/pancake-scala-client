package com.pancakedb.client

import com.pancakedb.idl.ColumnMeta

private[client] case class RawColumn(
  rowCount: Int,
  implicitNullsCount: Int,
  uncompressedBytes: Array[Byte],
  compressedBytes: Array[Byte],
  columnMeta: ColumnMeta,
  codec: String,
)

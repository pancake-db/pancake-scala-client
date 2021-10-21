package com.pancakedb.client

import com.pancakedb.idl.ColumnMeta

case class RawColumn(
  rowCount: Int,
  uncompressedBytes: Array[Byte],
  compressedBytes: Array[Byte],
  columnMeta: ColumnMeta,
  codec: String,
)

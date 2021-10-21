package com.pancakedb.client

import com.pancakedb.idl.FieldValue

import scala.collection.mutable.ArrayBuffer

trait PrimitiveHandler[A, P] {
  val isAtomic: Boolean

  def decodeNativeRepLevelsColumn(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    limit: Int,
    codec: String,
  ): NativeRepLevelsColumn[A]

  def decodeRepLevelsColumn(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    nRows: Int,
    codec: String,
  ): RepLevelsColumn[A] = {
    val nativeColumn = decodeNativeRepLevelsColumn(
      nestedListDepth,
      compressedBytes,
      uncompressedBytes,
      nRows,
      codec,
    )
    RepLevelsColumn(
      nativeColumn.atoms,
      nativeColumn.repLevels,
      nestedListDepth,
      nRows
    )
  }

  def atomToPrimitive(atom: A): P = {
    throw new UnsupportedOperationException("a non-atomic primitive cannot convert a single atom")
  }

  def combineAtoms(atoms: Array[A]): P = {
    throw new UnsupportedOperationException("an atomic primitive cannot combine atoms")
  }

  def setValue(p: P, builder: FieldValue.Builder): Unit

  def decodeFieldValues(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    nRows: Int,
    codec: String,
  ): ArrayBuffer[FieldValue] = {
    val column = decodeRepLevelsColumn(
      nestedListDepth,
      compressedBytes,
      uncompressedBytes,
      nRows,
      codec,
    )
    AtomNester(this, column).computeFieldValues(nRows)
  }
}

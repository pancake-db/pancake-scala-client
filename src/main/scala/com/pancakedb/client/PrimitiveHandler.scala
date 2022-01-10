package com.pancakedb.client

import com.pancakedb.idl.FieldValue

import scala.collection.mutable.ArrayBuffer

trait PrimitiveHandler[A, P] {
  private val NullRepLevel = 0.toByte
  private val NewRowRepLevels = Set(NullRepLevel, 1).map(_.toByte)

  val isAtomic: Boolean

  protected def decodeNativeRepLevelsColumn(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): NativeRepLevelsColumn[A]

  // this incorporates some metadata and filters out deletions
  def filterToRepLevelsColumn(
    nativeColumn: NativeRepLevelsColumn[A],
    nestedListDepth: Byte,
    rowCount: Int,
    implicitNullsCount: Int,
    deletions: Array[Boolean],
  ): RepLevelsColumn[A] = {
    val atomRepLevel = nestedListDepth + (if (isAtomic) 1 else 2)
    val filteredAtoms = ArrayBuffer.empty[A]
    val filteredRepLevels = ArrayBuffer.empty[Byte]
    var repIdx = 0
    var atomIdx = 0
    var rowIdx = 0
    var count = 0
    // first account for implicit nulls
    while (rowIdx < implicitNullsCount) {
      val notDeleted = rowIdx >= deletions.length || !deletions(rowIdx)
      if (notDeleted) {
        filteredRepLevels += NullRepLevel
        count += 1
      }
      rowIdx += 1
    }
    // then account for explicit data
    while (count < rowCount) {
      var rowFinished = false
      val notDeleted = rowIdx >= deletions.length || !deletions(rowIdx)
      while (!rowFinished) {
        val r = nativeColumn.repLevels(repIdx)
        val isNewAtom = r == atomRepLevel
        if (notDeleted) {
          filteredRepLevels += r
          if (isNewAtom) {
            filteredAtoms += nativeColumn.atoms(atomIdx)
          }
        }
        repIdx += 1
        if (isNewAtom) {
          atomIdx += 1
        }
        if (NewRowRepLevels.contains(r)) {
          rowFinished = true
        }
      }
      if (notDeleted) {
        count += 1
      }
      rowIdx += 1
    }

    RepLevelsColumn[A](
      makeAtomArray(filteredAtoms),
      filteredRepLevels.toArray,
      nestedListDepth,
      rowCount
    )
  }

  def decodeRepLevelsColumn(
    rawColumn: RawColumn,
    deletions: Array[Boolean],
  ): RepLevelsColumn[A] = {
    val nestedListDepth = rawColumn.columnMeta.getNestedListDepth.toByte
    val nativeColumn = decodeNativeRepLevelsColumn(
      nestedListDepth,
      rawColumn.compressedBytes,
      rawColumn.uncompressedBytes,
      rawColumn.codec,
    )
    filterToRepLevelsColumn(nativeColumn, nestedListDepth, rawColumn.rowCount, rawColumn.implicitNullsCount, deletions)
  }

  def atomToPrimitive(atom: A): P = {
    throw new UnsupportedOperationException("a non-atomic primitive cannot convert a single atom")
  }

  def combineAtoms(atoms: Array[A]): P = {
    throw new UnsupportedOperationException("an atomic primitive cannot combine atoms")
  }

  def setValue(p: P, builder: FieldValue.Builder): Unit

  def decodeFieldValues(
    rawColumn: RawColumn,
    deletions: Array[Boolean],
  ): ArrayBuffer[FieldValue] = {
    val column = decodeRepLevelsColumn(
      rawColumn,
      deletions,
    )
    AtomNester(this, column).computeFieldValues(rawColumn.rowCount)
  }

  def makeAtomArray(buffer: ArrayBuffer[A]): Array[A]
}

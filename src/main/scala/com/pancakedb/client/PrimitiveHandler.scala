package com.pancakedb.client

import com.pancakedb.idl.FieldValue

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait PrimitiveHandler[A, P] {
  private val NewRowRepLevels = Set(0, 1).map(_.toByte)

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
    nRows: Int,
    deletions: Array[Boolean],
  ): RepLevelsColumn[A] = {
    val atomRepLevel = nestedListDepth + (if (isAtomic) 1 else 2)
    val filteredAtoms = ArrayBuffer.empty[A]
    val filteredRepLevels = ArrayBuffer.empty[Byte]
    var repIdx = 0
    var atomIdx = 0
    var rowIdx = 0
    var count = 0
    while (count < nRows) {
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
      nRows
    )
  }

  def decodeRepLevelsColumn(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    nRows: Int,
    codec: String,
    deletions: Array[Boolean],
  )(implicit ev: ClassTag[A]): RepLevelsColumn[A] = {
    val nativeColumn = decodeNativeRepLevelsColumn(
      nestedListDepth,
      compressedBytes,
      uncompressedBytes,
      codec,
    )
    filterToRepLevelsColumn(nativeColumn, nestedListDepth, nRows, deletions)
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
    deletions: Array[Boolean],
  )(implicit ev: ClassTag[A]): ArrayBuffer[FieldValue] = {
    val column = decodeRepLevelsColumn(
      nestedListDepth,
      compressedBytes,
      uncompressedBytes,
      nRows,
      codec,
      deletions
    )
    AtomNester(this, column).computeFieldValues(nRows)
  }

  def makeAtomArray(buffer: ArrayBuffer[A]): Array[A]
}

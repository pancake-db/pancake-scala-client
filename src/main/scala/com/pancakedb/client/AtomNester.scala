package com.pancakedb.client

import Exceptions.CorruptDataException
import com.pancakedb.idl.{FieldValue, RepeatedFieldValue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[client] case class AtomNester[A, P](
  handler: PrimitiveHandler[A, P],
  repLevelsColumn: RepLevelsColumn[A],
) {
  var repIdx = 0
  var atomIdx = 0

  def nextFieldValue(depth: Int = 0): FieldValue = {
    var level = repLevelsColumn.repLevels(repIdx)
    if (depth == 0 && level == 0) {
      // null
      repIdx += 1
      FieldValue.newBuilder().build()
    } else if (depth < repLevelsColumn.nestingDepth) {
      // list
      val subValues = ArrayBuffer.empty[FieldValue]
      while (level > depth + 1) {
        subValues += nextFieldValue(depth + 1)
        level = repLevelsColumn.repLevels(repIdx)
      }

      if (level == depth + 1) {
        repIdx += 1
      }
      FieldValue.newBuilder()
        .setListVal(
          RepeatedFieldValue.newBuilder().addAllVals(subValues.asJava)
        )
        .build()
    } else if (depth == repLevelsColumn.nestingDepth) {
      // primitive
      val primitive = if (handler.isAtomic) {
        val res = handler.atomToPrimitive(repLevelsColumn.atoms(atomIdx))
        repIdx += 1
        atomIdx += 1
        res
      } else {
        val start = atomIdx
        while (level == repLevelsColumn.nestingDepth + 2) {
          repIdx += 1
          atomIdx += 1
          level = repLevelsColumn.repLevels(repIdx)
        }
        repIdx += 1
        val atoms = repLevelsColumn.atoms.slice(start, atomIdx)
        handler.combineAtoms(atoms)
      }
      val builder = FieldValue.newBuilder()
      handler.setValue(primitive, builder)
      builder.build()
    } else {
      throw CorruptDataException(s"invalid repetition level $level at depth $depth")
    }
  }

  def computeFieldValues(limit: Int): ArrayBuffer[FieldValue] = {
    val res = ArrayBuffer.empty[FieldValue]
    var count = 0
    while (repIdx < repLevelsColumn.repLevels.length && count < limit) {
      res += nextFieldValue()
      count += 1
    }
    res
  }
}

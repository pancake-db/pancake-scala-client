package com.pancakedb.client

import com.github.sbt.jni.syntax.NativeLoader

private[client] object NativeCore extends NativeLoader("pancake_scala_client_native") {
  case class LongColumn(atoms: Array[Long], repLevels: Array[Byte])
    extends NativeRepLevelsColumn[Long]
  case class BooleanColumn(atoms: Array[Boolean], repLevels: Array[Byte])
    extends NativeRepLevelsColumn[Boolean]
  case class FloatColumn(atoms: Array[Float], repLevels: Array[Byte])
    extends NativeRepLevelsColumn[Float]
  case class DoubleColumn(atoms: Array[Double], repLevels: Array[Byte])
    extends NativeRepLevelsColumn[Double]
  case class StringOrBytesColumn(atoms: Array[Byte], repLevels: Array[Byte])
    extends NativeRepLevelsColumn[Byte]

  @native
  def decodeInt64s(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): LongColumn

  @native
  def decodeBools(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): BooleanColumn

  @native
  def decodeFloat32s(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): FloatColumn

  @native
  def decodeFloat64s(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): DoubleColumn

  // Spark encodes timestamps as a single long of epoch micros.
  // For now we stick with that convention.
  @native
  def decodeTimestamps(
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): LongColumn

  @native
  def decodeStringOrBytes(
    dtype: String,
    nestedListDepth: Byte,
    compressedBytes: Array[Byte],
    uncompressedBytes: Array[Byte],
    codec: String,
  ): StringOrBytesColumn

  @native
  def decodeDeletions(
    data: Array[Byte],
  ): Array[Boolean]
}

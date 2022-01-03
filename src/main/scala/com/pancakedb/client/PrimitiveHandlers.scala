package com.pancakedb.client

import com.google.protobuf.{ByteString, Timestamp}
import Exceptions.UnrecognizedDataTypeException
import com.pancakedb.idl.{DataType, FieldValue}

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

object PrimitiveHandlers {
  def getHandler(dtype: DataType): PrimitiveHandler[_, _] = {
    dtype match {
      case DataType.INT64 => Int64Handler
      case DataType.STRING => StringHandler
      case DataType.BOOL => BoolHandler
      case DataType.FLOAT32 => Float32Handler
      case DataType.FLOAT64 => Float64Handler
      case DataType.TIMESTAMP_MICROS => TimestampMicrosHandler
      case DataType.BYTES => BytesHandler
      case DataType.UNRECOGNIZED => throw UnrecognizedDataTypeException
    }
  }

  object Int64Handler extends PrimitiveHandler[Long, Long] {
    override val isAtomic: Boolean = true
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Long] = {
      NativeCore.decodeInt64s(
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def atomToPrimitive(atom: Long): Long = atom
    override def setValue(p: Long, builder: FieldValue.Builder): Unit = {
      builder.setInt64Val(p)
    }
    override def makeAtomArray(buffer: ArrayBuffer[Long]): Array[Long] = buffer.toArray
  }

  object BoolHandler extends PrimitiveHandler[Boolean, Boolean] {
    override val isAtomic: Boolean = true
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Boolean] = {
      NativeCore.decodeBools(
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def atomToPrimitive(atom: Boolean): Boolean = atom
    override def setValue(p: Boolean, builder: FieldValue.Builder): Unit = {
      builder.setBoolVal(p)
    }
    override def makeAtomArray(buffer: ArrayBuffer[Boolean]): Array[Boolean] = buffer.toArray
  }

  object Float32Handler extends PrimitiveHandler[Float, Float] {
    override val isAtomic: Boolean = true
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Float] = {
      NativeCore.decodeFloat32s(
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def atomToPrimitive(atom: Float): Float = atom
    override def setValue(p: Float, builder: FieldValue.Builder): Unit = {
      builder.setFloat32Val(p)
    }
    override def makeAtomArray(buffer: ArrayBuffer[Float]): Array[Float] = buffer.toArray
  }

  object Float64Handler extends PrimitiveHandler[Double, Double] {
    override val isAtomic: Boolean = true
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Double] = {
      NativeCore.decodeFloat64s(
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def atomToPrimitive(atom: Double): Double = atom
    override def setValue(p: Double, builder: FieldValue.Builder): Unit = {
      builder.setFloat64Val(p)
    }
    override def makeAtomArray(buffer: ArrayBuffer[Double]): Array[Double] = buffer.toArray
  }

  object TimestampMicrosHandler extends PrimitiveHandler[Long, Timestamp] {
    override val isAtomic: Boolean = true
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Long] = {
      NativeCore.decodeTimestamps(
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def atomToPrimitive(atom: Long): Timestamp = {
      Timestamp.newBuilder()
        .setSeconds(Math.floorDiv(atom, 1000000))
        .setNanos(Math.floorMod(atom, 1000000) * 1000)
        .build()
    }
    override def setValue(p: Timestamp, builder: FieldValue.Builder): Unit = {
      builder.setTimestampVal(p)
    }
    override def makeAtomArray(buffer: ArrayBuffer[Long]): Array[Long] = buffer.toArray
  }

  object BytesHandler extends PrimitiveHandler[Byte, Array[Byte]] {
    override val isAtomic: Boolean = false
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Byte] = {
      NativeCore.decodeStringOrBytes(
        DataType.BYTES.toString,
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def combineAtoms(atoms: Array[Byte]): Array[Byte] = atoms
    override def setValue(p: Array[Byte], builder: FieldValue.Builder): Unit = {
      builder.setBytesVal(ByteString.copyFrom(p))
    }
    override def makeAtomArray(buffer: ArrayBuffer[Byte]): Array[Byte] = buffer.toArray
  }

  object StringHandler extends PrimitiveHandler[Byte, String] {
    override val isAtomic: Boolean = false
    override def decodeNativeRepLevelsColumn(
      nestedListDepth: Byte,
      compressedBytes: Array[Byte],
      uncompressedBytes: Array[Byte],
      codec: String
    ): NativeRepLevelsColumn[Byte] = {
      NativeCore.decodeStringOrBytes(
        DataType.STRING.toString,
        nestedListDepth,
        compressedBytes,
        uncompressedBytes,
        codec,
      )
    }
    override def combineAtoms(atoms: Array[Byte]): String = new String(atoms, StandardCharsets.UTF_8)
    override def setValue(p: String, builder: FieldValue.Builder): Unit = {
      builder.setStringVal(p)
    }
    override def makeAtomArray(buffer: ArrayBuffer[Byte]): Array[Byte] = buffer.toArray
  }
}

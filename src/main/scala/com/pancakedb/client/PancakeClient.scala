package com.pancakedb.client

import com.pancakedb.client.Exceptions.CorruptDataException
import com.pancakedb.client.PancakeClient.{DetailedRepLevelsColumn, generateCorrelationId}
import com.pancakedb.idl._
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.existentials

case class PancakeClient(host: String, port: Int) {
  @transient private lazy val channel: ManagedChannel = {
    val builder = ManagedChannelBuilder
      .forAddress(host, port)
    builder.usePlaintext()
    builder.build()
  }
  @transient lazy val grpc: PancakeDbGrpc.PancakeDbFutureStub = PancakeDbGrpc.newFutureStub(channel)
  @transient private lazy val grpcStub: PancakeDbGrpc.PancakeDbStub = PancakeDbGrpc.newStub(channel)

  private case class ReadSegmentColumnObserver(
    columnMeta: ColumnMeta,
  ) extends StreamObserver[ReadSegmentColumnResponse] {
    var first: Boolean = true
    var rowCount: Int = 0
    var implicitNullsCount: Int = 0
    val uncompressedBytes: ArrayBuffer[Byte] = ArrayBuffer.empty
    val compressedBytes: ArrayBuffer[Byte] = ArrayBuffer.empty
    var codec: String = ""
    val promise: Promise[RawColumn] = Promise()

    override def onNext(resp: ReadSegmentColumnResponse): Unit = {
      if (first) {
        rowCount = resp.getRowCount
        implicitNullsCount = resp.getImplicitNullsCount
      }
      if (resp.getCodec.nonEmpty) {
        if (resp.getImplicitNullsCount > 0) {
          promise.failure(CorruptDataException(
            s"contradictory ReadSegmentColumn response (dtype ${columnMeta.getDtype}) " +
              "contains both compacted data and implicit nulls"
          ))
        }
        codec = resp.getCodec
        compressedBytes ++= resp.getData.toByteArray
      } else {
        uncompressedBytes ++= resp.getData.toByteArray
      }

      first = false
    }

    override def onError(t: Throwable): Unit = {
      promise.failure(t)
    }

    override def onCompleted(): Unit = {
      promise.success(RawColumn(
        rowCount,
        implicitNullsCount,
        uncompressedBytes.toArray,
        compressedBytes.toArray,
        columnMeta,
        codec,
      ))
    }
  }

  private def readSegmentRawColumn(
    tableName: String,
    partition: scala.collection.Map[String, PartitionFieldValue],
    segmentId: String,
    columnName: String,
    columnMeta: ColumnMeta,
    correlationId: String,
  ): Future[RawColumn] = {
    val req = ReadSegmentColumnRequest.newBuilder()
      .setTableName(tableName)
      .setSegmentId(segmentId)
      .putAllPartition(partition.asJava)
      .setColumnName(columnName)
      .setCorrelationId(correlationId)
      .build()

    val observer = ReadSegmentColumnObserver(columnMeta)

    grpcStub.readSegmentColumn(req, observer)

    observer.promise.future
  }

  private def decodeSegmentRepLevelsDetailed(
    tableName: String,
    partition: scala.collection.Map[String, PartitionFieldValue],
    segmentId: String,
    columns: scala.collection.Map[String, ColumnMeta],
  )(implicit ec: ExecutionContext): Map[String, DetailedRepLevelsColumn[_, _]] = {
    if (columns.isEmpty) {
      throw new IllegalArgumentException(s"decodeSegment requires at least one column to decode")
    }

    val correlationId = generateCorrelationId()

    val req = ReadSegmentDeletionsRequest.newBuilder()
      .setTableName(tableName)
      .putAllPartition(partition.asJava)
      .setSegmentId(segmentId)
      .setCorrelationId(correlationId)
      .build()
    val deletionFuture = grpc.readSegmentDeletions(req)
    val columnFutures = columns.map({case (name, meta) =>
      val future = readSegmentRawColumn(
        tableName,
        partition,
        segmentId,
        name,
        meta,
        correlationId,
      )
      (name, future)
    })

    val deletions = {
      val bytes = deletionFuture.get().getData.toByteArray
      NativeCore.decodeDeletions(bytes)
    }

    columnFutures.map({case (name, rawColumnFuture) =>
      val rawColumn = Await.result(rawColumnFuture, Duration.Inf)
      val meta = columns(name)
      val handler = PrimitiveHandlers.getHandler(meta.getDtype)
      val detailed = DetailedRepLevelsColumn.from(
        handler,
        rawColumn,
        deletions,
      )
      (name, detailed)
    }).toMap
  }

  // This returns a mid-level representation of the table.
  // Each rep levels column may have a different number of rows.
  def decodeSegmentRepLevelsColumns(
    tableName: String,
    partition: scala.collection.Map[String, PartitionFieldValue],
    segmentId: String,
    columns: scala.collection.Map[String, ColumnMeta],
  )(implicit ec: ExecutionContext): Map[String, RepLevelsColumn[_]] = {
    val detailed = decodeSegmentRepLevelsDetailed(tableName, partition, segmentId, columns)
    detailed.mapValues(_.repLevelsColumn)
  }

  // This returns a high-level representation of the table.
  def decodeSegment(
    tableName: String,
    partition: scala.collection.Map[String, PartitionFieldValue],
    segmentId: String,
    columns: scala.collection.Map[String, ColumnMeta],
  )(implicit ec: ExecutionContext): Array[Row] = {
    val repLevelsColumns = decodeSegmentRepLevelsDetailed(
      tableName,
      partition,
      segmentId,
      columns,
    )
    val n = repLevelsColumns
      .values
      .map(_.repLevelsColumn.nRows)
      .min

    val fvColumns = repLevelsColumns
      .map({case (name, detailed) =>
        (name, detailed.toAtomNester.computeFieldValues(n))
      })

    (0 until n).toArray.map(rowIdx => {
      val row = Row.newBuilder()
      fvColumns.foreach({case (name, fvs) =>
        row.putFields(name, fvs(rowIdx))
      })
      row.build()
    })
  }

  def shutdown(): Unit = {
    channel.shutdown()
  }
}

object PancakeClient {
  private def generateCorrelationId(): String = {
    UUID.randomUUID().toString
  }

  private case class DetailedRepLevelsColumn[A, P](
    handler: PrimitiveHandler[A, P],
    repLevelsColumn: RepLevelsColumn[A],
  ) {
    def toAtomNester: AtomNester[A, P] = AtomNester(handler, repLevelsColumn)
  }

  private object DetailedRepLevelsColumn {
    def from[A, P](
      handler: PrimitiveHandler[A, P],
      rawColumn: RawColumn,
      deletions: Array[Boolean],
    ): DetailedRepLevelsColumn[A, P] = {
      val repLevelsColumn = handler.decodeRepLevelsColumn(
        rawColumn,
        deletions,
      )
      DetailedRepLevelsColumn(handler, repLevelsColumn)
    }
  }
}

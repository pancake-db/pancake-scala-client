package com.pancakedb.client

import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{ByteString, Message, MessageOrBuilder}
import com.pancakedb.client.Exceptions.{CorruptDataException, HttpException}
import com.pancakedb.client.PancakeClient.{DetailedRepLevelsColumn, JSON_BYTE_DELIMITER, generateCorrelationId}
import com.pancakedb.idl._
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpPost, RequestBuilder}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClientBuilder

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.MapLike
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class PancakeClient(host: String, port: Int) {
  @transient lazy val httpClient: HttpClient = {
    HttpClientBuilder.create()
        .build()
  }

  case object Api {
    // mutably modifies response builder, returns spare bytes
    private def jsonRequest(
      methodName: String,
      routeName: String,
      req: MessageOrBuilder,
      respBuilder: Message.Builder,
    ): Option[Array[Byte]] = {
      val body = JsonFormat.printer().print(req)
      val uri = URI.create(s"http://$host:$port/rest/$routeName")
      val entity = new StringEntity(
        body,
        ContentType.APPLICATION_JSON
      )
      val httpReq = RequestBuilder.create(methodName)
        .setUri(uri)
        .setEntity(entity)
        .build()
      val httpResp = httpClient.execute(httpReq)
      val allBytes = httpResp.getEntity.getContent.readAllBytes()
      val statusCode = httpResp.getStatusLine.getStatusCode
      if (statusCode != 200) {
        val responseString = try {
          new String(allBytes)
        } catch {
          case _: IllegalArgumentException => "<non-UTF8 bytes>"
        }
        throw HttpException(
          statusCode,
          s"""HTTP request to Pancake server route $routeName received response
             |with code $statusCode:\n$responseString""".stripMargin
        )
      }
      val idx = allBytes.indexOfSlice(JSON_BYTE_DELIMITER)
      if (idx >= 0) {
        val respBody = new String(allBytes.slice(0, idx + 1))
        JsonFormat.parser().merge(respBody, respBuilder)

        val extraBytes = allBytes.slice(idx + 2, allBytes.length)
        Some(extraBytes)
      } else {
        val respBody = new String(allBytes)
        JsonFormat.parser().merge(respBody, respBuilder)
        None
      }
    }

    def createTable(req: CreateTableRequest): CreateTableResponse = {
      val respBuilder = CreateTableResponse.newBuilder()
      jsonRequest(HttpPost.METHOD_NAME, "create_table", req, respBuilder)
      respBuilder.build()
    }

    def dropTable(req: DropTableRequest): DropTableResponse = {
      val respBuilder = DropTableResponse.newBuilder()
      jsonRequest(HttpPost.METHOD_NAME, "drop_table", req, respBuilder)
      respBuilder.build()
    }

    def getSchema(req: GetSchemaRequest): GetSchemaResponse = {
      val respBuilder = GetSchemaResponse.newBuilder()
      jsonRequest(HttpGet.METHOD_NAME, "get_schema", req, respBuilder)
      respBuilder.build()
    }

    def listSegments(req: ListSegmentsRequest): ListSegmentsResponse = {
      val respBuilder = ListSegmentsResponse.newBuilder()
      jsonRequest(HttpGet.METHOD_NAME, "list_segments", req, respBuilder)
      respBuilder.build()
    }

    def readSegmentColumn(req: ReadSegmentColumnRequest): ReadSegmentColumnResponse = {
      val respBuilder = ReadSegmentColumnResponse.newBuilder()
      val extraBytes = jsonRequest(HttpGet.METHOD_NAME, "read_segment_column", req, respBuilder)
      val byteString = ByteString.copyFrom(extraBytes.get)
      respBuilder.setData(byteString)
      respBuilder.build()
    }

    def writeToPartition(req: WriteToPartitionRequest): WriteToPartitionResponse = {
      val respBuilder = WriteToPartitionResponse.newBuilder()
      jsonRequest(HttpPost.METHOD_NAME, "write_to_partition", req, respBuilder)
      respBuilder.build()
    }

    def readSegmentDeletions(req: ReadSegmentDeletionsRequest): ReadSegmentDeletionsResponse = {
      val respBuilder = ReadSegmentDeletionsResponse.newBuilder()
      val extraBytes = jsonRequest(HttpGet.METHOD_NAME, "read_segment_deletions", req, respBuilder)
      val byteString = ByteString.copyFrom(extraBytes.get)
      respBuilder.setData(byteString)
      respBuilder.build()
    }

    def deleteFromSegment(req: DeleteFromSegmentRequest): DeleteFromSegmentResponse = {
      val respBuilder = DeleteFromSegmentResponse.newBuilder()
      jsonRequest(HttpPost.METHOD_NAME, "delete_from_segment", req, respBuilder)
      respBuilder.build()
    }

    def alterTable(req: AlterTableRequest): AlterTableResponse = {
      val respBuilder = AlterTableResponse.newBuilder()
      jsonRequest(HttpPost.METHOD_NAME, "alter_table", req, respBuilder)
      respBuilder.build()
    }
  }

  private def readSegmentRawColumn(
    tableName: String,
    partition: scala.collection.Map[String, PartitionFieldValue],
    segmentId: String,
    columnName: String,
    columnMeta: ColumnMeta,
    correlationId: String,
  ): RawColumn = {
    var continuation = ""
    var first = true

    var rowCount = 0
    var codec = ""
    var implicitNullsCount = 0
    val uncompressedBytes = ArrayBuffer.empty[Byte]
    val compressedBytes = ArrayBuffer.empty[Byte]

    while (continuation.nonEmpty || first) {
      val req = ReadSegmentColumnRequest.newBuilder()
        .setTableName(tableName)
        .setSegmentId(segmentId)
        .putAllPartition(partition.asJava)
        .setColumnName(columnName)
        .setCorrelationId(correlationId)
        .setContinuationToken(continuation)
        .build()
      val resp = Api.readSegmentColumn(req)

      if (first) {
        rowCount = resp.getRowCount
        implicitNullsCount = resp.getImplicitNullsCount
      }
      if (resp.getCodec.nonEmpty) {
        if (resp.getImplicitNullsCount > 0) {
          throw CorruptDataException(
            s"""contradictory response for $tableName $segmentId $columnName
            contains both compacted data and implicit nulls"""
          )
        }
        codec = resp.getCodec
        compressedBytes ++= resp.getData.toByteArray
      } else {
        uncompressedBytes ++= resp.getData.toByteArray
      }

      first = false
      continuation = resp.getContinuationToken
    }

    RawColumn(
      rowCount,
      implicitNullsCount,
      uncompressedBytes.toArray,
      compressedBytes.toArray,
      columnMeta,
      codec,
    )
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

    val deletionFuture = Future {
      val req = ReadSegmentDeletionsRequest.newBuilder()
        .setTableName(tableName)
        .putAllPartition(partition.asJava)
        .setSegmentId(segmentId)
        .setCorrelationId(correlationId)
        .build()
      Api.readSegmentDeletions(req)
    }
    val columnFutures = columns.map({case (name, meta) =>
      val future = Future {
        readSegmentRawColumn(
          tableName,
          partition,
          segmentId,
          name,
          meta,
          correlationId,
        )
      }
      (name, future)
    })

    val deletions = {
      val bytes = Await.result(deletionFuture, Duration.Inf).getData.toByteArray
      // TODO get rid of this check once core library handles it instead
      if (bytes.isEmpty) {
        Array.empty[Boolean]
      } else {
        NativeCore.decodeDeletions(bytes)
      }
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
}

object PancakeClient {
  private val JSON_BYTE_DELIMITER: Array[Byte] = "}\n".getBytes(StandardCharsets.UTF_8)

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

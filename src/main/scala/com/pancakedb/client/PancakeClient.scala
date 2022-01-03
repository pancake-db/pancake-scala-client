package com.pancakedb.client

import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{ByteString, Message, MessageOrBuilder}
import com.pancakedb.client.Exceptions.HttpException
import com.pancakedb.client.PancakeClient.JSON_BYTE_DELIMITER
import com.pancakedb.idl._
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpPost, RequestBuilder}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClientBuilder

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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
    partition: Map[String, PartitionFieldValue],
    segmentId: String,
    columnName: String,
    columnMeta: ColumnMeta,
    correlationId: String,
  ): RawColumn = {
    var continuation = ""
    var first = true

    var rowCount = 0
    var codec = ""
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
      }
      if (resp.getCodec.nonEmpty) {
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
      uncompressedBytes.toArray,
      compressedBytes.toArray,
      columnMeta,
      codec,
    )
  }

  def decodeSegmentDeletions(
    tableName: String,
    partition: Map[String, PartitionFieldValue],
    segmentId: String,
    correlationId: String,
  ): Array[Boolean] = {
    val req = ReadSegmentDeletionsRequest.newBuilder()
      .setTableName(tableName)
      .putAllPartition(partition.asJava)
      .setSegmentId(segmentId)
      .setCorrelationId(correlationId)
      .build()
    val resp = this.Api.readSegmentDeletions(req)
    val bytes = resp.getData.toByteArray
    // TODO get rid of this check once core library handles it instead
    if (bytes.isEmpty) {
      Array.empty
    } else {
      NativeCore.decodeDeletions(resp.getData.toByteArray)
    }
  }

  def decodeSegmentRepLevelsColumn(
    tableName: String,
    partition: Map[String, PartitionFieldValue],
    segmentId: String,
    columnName: String,
    columnMeta: ColumnMeta,
    correlationId: String,
    deletions: Array[Boolean],
  ): RepLevelsColumn[_] = {
    val rawSegmentColumn = readSegmentRawColumn(
      tableName,
      partition,
      segmentId,
      columnName,
      columnMeta,
      correlationId,
    )

    val handler = PrimitiveHandlers.getHandler(columnMeta.getDtype)
    handler.decodeRepLevelsColumn(
      columnMeta.getNestedListDepth.toByte,
      rawSegmentColumn.compressedBytes,
      rawSegmentColumn.uncompressedBytes,
      rawSegmentColumn.rowCount,
      rawSegmentColumn.codec,
      deletions,
    )
  }

  private def decodeSegmentColumn(
    tableName: String,
    partition: Map[String, PartitionFieldValue],
    segmentId: String,
    columnName: String,
    columnMeta: ColumnMeta,
    correlationId: String,
    deletions: Array[Boolean],
  ): ArrayBuffer[FieldValue] = {
    val rawSegmentColumn = readSegmentRawColumn(
      tableName,
      partition,
      segmentId,
      columnName,
      columnMeta,
      correlationId,
    )

    val handler = PrimitiveHandlers.getHandler(columnMeta.getDtype)
    handler.decodeFieldValues(
      columnMeta.getNestedListDepth.toByte,
      rawSegmentColumn.compressedBytes,
      rawSegmentColumn.uncompressedBytes,
      rawSegmentColumn.rowCount,
      rawSegmentColumn.codec,
      deletions
    )
  }

  // this can be made faster by parallel execution of the decodeSegmentColumn calls
  def decodeSegment(
    tableName: String,
    partition: Map[String, PartitionFieldValue],
    segmentId: String,
    columnMetas: Map[String, ColumnMeta],
  ): Array[Row] = {
    val correlationId = PancakeClient.generateCorrelationId()

    if (columnMetas.isEmpty) {
      throw new IllegalArgumentException(s"decodeSegment requires at least one column to decode")
    }

    val deletions = decodeSegmentDeletions(
      tableName,
      partition,
      segmentId,
      correlationId,
    )

    val segmentColumns = columnMetas.map({case (columnName, meta) =>
      (
        columnName,
        decodeSegmentColumn(tableName, partition, segmentId, columnName, meta, correlationId, deletions)
      )
    })
    val n = segmentColumns.values.map(_.length).min
    (0 until n).toArray.map(rowIdx => {
      val row = Row.newBuilder()
      segmentColumns.foreach({case (columnName, fvs) =>
        row.putFields(columnName, fvs(rowIdx))
      })
      row.build()
    })
  }
}

object PancakeClient {
  private val JSON_BYTE_DELIMITER: Array[Byte] = "}\n".getBytes(StandardCharsets.UTF_8)

  def generateCorrelationId(): String = {
    UUID.randomUUID().toString
  }
}

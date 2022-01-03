package com.pancakedb.client

import com.pancakedb.client.Exceptions.HttpException
import com.pancakedb.idl.{ColumnMeta, CreateTableRequest, DataType, DeleteFromSegmentRequest, DropTableRequest, FieldValue, ListSegmentsRequest, Row, Schema, WriteToPartitionRequest}

class IntegrationTest extends TestBase {
  val TableName = "jvm_client_test_table"
  val ColumnName = "s"
  val Meta: ColumnMeta = ColumnMeta.newBuilder()
    .setDtype(DataType.STRING)
    .build()

  "writing data and reading it back" should "give the same result" ignore {
    val client = PancakeClient("localhost", 1337)
    try {
      client.Api.dropTable(DropTableRequest.newBuilder()
        .setTableName(TableName)
        .build())
    } catch {
      case HttpException(404, _) =>
    }

    client.Api.createTable(CreateTableRequest.newBuilder()
      .setTableName(TableName)
      .setSchema(Schema.newBuilder().putColumns(ColumnName, Meta))
      .build()
    )

    val writeReq = WriteToPartitionRequest.newBuilder()
      .setTableName(TableName)

    val strings = Array(
      "a",
      "bc",
      "def",
      "",
    )
    strings.foreach(s => writeReq.addRows(
      Row.newBuilder().putFields(
        ColumnName,
        FieldValue.newBuilder().setStringVal(s).build()
      ).build()
    ))
    client.Api.writeToPartition(writeReq.build())

    val segment = client.Api.listSegments(ListSegmentsRequest.newBuilder()
      .setTableName(TableName)
      .setIncludeMetadata(true)
      .build()
    ).getSegments(0)
    assertResult(strings.length)(segment.getMetadata.getRowCount)

    val segmentId = segment.getSegmentId
    client.Api.deleteFromSegment(DeleteFromSegmentRequest.newBuilder()
      .setTableName(TableName)
      .setSegmentId(segmentId)
      .addRowIds(0)
      .addRowIds(2)
      .build()
    )

    val expectedStrings = Array("bc", "")
    val rows = client.decodeSegment(
      TableName,
      Map.empty,
      segmentId,
      Map(ColumnName -> Meta)
    )
    assertResult(expectedStrings)(rows.map(_.getFieldsMap.get(ColumnName).getStringVal))
  }
}

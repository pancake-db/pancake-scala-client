package com.pancakedb.client

import com.pancakedb.client.Exceptions.HttpException
import com.pancakedb.idl._

import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends TestBase {
  val TableName = "jvm_client_test_table"
  val OrigColumnName = "s"
  val OrigMeta: ColumnMeta = ColumnMeta.newBuilder()
    .setDtype(DataType.STRING)
    .build()
  val AddColumnName = "new"
  val AddMeta: ColumnMeta = ColumnMeta.newBuilder()
    .setDtype(DataType.BOOL)
    .build()

  "writing data and reading it back" should "give the same result" in {
    val client = PancakeClient("localhost", 3841)
    try {
      client.Api.dropTable(DropTableRequest.newBuilder()
        .setTableName(TableName)
        .build())
    } catch {
      case HttpException(404, _) =>
    }

    client.Api.createTable(CreateTableRequest.newBuilder()
      .setTableName(TableName)
      .setSchema(Schema.newBuilder().putColumns(OrigColumnName, OrigMeta))
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
        OrigColumnName,
        FieldValue.newBuilder().setStringVal(s).build()
      ).build()
    ))
    client.Api.writeToPartition(writeReq.build())

    client.Api.alterTable(AlterTableRequest.newBuilder()
      .setTableName(TableName)
      .putNewColumns(AddColumnName, AddMeta)
      .build()
    )

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
      Map(OrigColumnName -> OrigMeta, AddColumnName -> AddMeta)
    )
    assertResult(expectedStrings)(rows.map(_.getFieldsMap.get(OrigColumnName).getStringVal))
    val implicitBoolFvs = rows.map(_.getFieldsMap.get(AddColumnName))
    for (fv <- implicitBoolFvs) {
      assertResult(FieldValue.ValueCase.VALUE_NOT_SET)(fv.getValueCase)
    }
  }
}

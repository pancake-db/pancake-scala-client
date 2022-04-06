package com.pancakedb.client

import com.pancakedb.idl._
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException

import java.util.concurrent.ExecutionException
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
    val client = PancakeClient("localhost", 3842)
    try {
      client.grpc.dropTable(DropTableRequest.newBuilder()
        .setTableName(TableName)
        .build()
      ).get()
    } catch {
      case e: StatusRuntimeException if e.getStatus.getCode == Code.NOT_FOUND =>
    }

    client.grpc.createTable(CreateTableRequest.newBuilder()
      .setTableName(TableName)
      .setSchema(Schema.newBuilder().putColumns(OrigColumnName, OrigMeta))
      .build()
    ).get()

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
    client.grpc.writeToPartition(writeReq.build()).get()

    client.grpc.alterTable(AlterTableRequest.newBuilder()
      .setTableName(TableName)
      .putNewColumns(AddColumnName, AddMeta)
      .build()
    ).get()

    val segment = client.grpc.listSegments(ListSegmentsRequest.newBuilder()
      .setTableName(TableName)
      .setIncludeMetadata(true)
      .build()
    ).get().getSegments(0)
    assertResult(strings.length)(segment.getMetadata.getRowCount)

    val segmentId = segment.getSegmentId
    client.grpc.deleteFromSegment(DeleteFromSegmentRequest.newBuilder()
      .setTableName(TableName)
      .setSegmentId(segmentId)
      .addRowIds(0)
      .addRowIds(2)
      .build()
    ).get()

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

    client.shutdown()

    // now that the client is shutdown, expect failures
    assertThrows[ExecutionException] {
      client.grpc.writeToPartition(writeReq.build()).get()
    }
  }
}

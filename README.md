[![Maven Central][maven-badge]][maven-url]

[maven-badge]: https://maven-badges.herokuapp.com/maven-central/com.pancakedb/pancake-db-client_2.12/badge.svg?gav=true
[maven-url]: https://search.maven.org/artifact/com.pancakedb/pancake-db-client_2.12

# Usage

## Requirements

Note that this library leverages PancakeDB Core,
a native rust library that needs to be compiled for each
architecture and OS.
The main release of this library pre-builds for some of these.
Therefore you will need to run on one of them:
* Darwin (Mac)
* x86_64 Linux, glibc>=2.31
* aarch64 Linux, glibc>=2.31

or compile your native binary and include it in your `resources/native/$ARCHITECTURE-$OS/libpancake_scala_client_native.$SUFFIX`.

You can add this client to your `build.sbt` or equivalent via something like
`libraryDependencies += "com.pancakedb" %% "pancake-db-client" % <version>`

## Creating a Client

Create a client instance via
```
import com.pancakedb.client.PancakeClient
val client = PancakeClient(
  "your_host",
  your_port,
)
val api = client.Api
```

## Essential API

Each of these calls simply sends a request to the server and parses the
response.
For details about the API calls and what all their fields mean,
see the [API docs](https://github.com/pancake-db/pancake-idl).

Import all the protobufs with `import com.pancakedb.idl._`

### Create Table
```
api.createTable(req: CreateTableRequest): CreateTableResponse
```

### Drop Table
```
api.dropTable(req: DropTableRequest): DropTableResponse
```

### Get Schema
```
api.getSchema(req: GetSchemaRequest): GetSchemaResponse
```


### List Segments
```
api.listSegments(req: ListSegmentsRequest): ListSegmentsResponse
```

### Write to Partition
```
api.writeToPartition(req: WriteToPartitionRequest): WriteToPartitionResponse
```

### Read Segment Column
```
api.readSegmentColumn(req: ReadSegmentColumnRequest): ReadSegmentColumnResponse
```

## Higher-level Functionality

The raw API for `read_segment_column` returns serialized bytes that aren't
immediately helpful.
To make sense of that data, the client supports:

### Decode Segment RepLevels Column

This reads the segment column, following continuation tokens.
It decodes to a `RepLevelsColumn`, containing an array of "atoms"
(e.g. `Long`s or `Byte`s) and "repetition levels" indicating which
rows have nulls and how these atoms are organized into strings or lists.

```
client.decodeSegmentRepLevelsColumn(
  tableName: String,
  partition: ArrayBuffer[PartitionField],
  segmentId: String,
  columnMeta: ColumnMeta,
  limit: Int = Int.MaxValue,
): RepLevelsColumn[_]
```

This is very computationally efficient, but typically leaves data too
unprocessed, so the next methods in this guide exist to make decoding easier.

### Decode Segment Column

This reads the segment column, following continuation tokens.
It decodes to an array buffer of `FieldValue`s, which contain deserialized data.

```
client.decodeSegmentColumn(
  tableName: String,
  partition: ArrayBuffer[PartitionField],
  segmentId: String,
  columnMeta: ColumnMeta,
  limit: Int = Int.MaxValue,
): ArrayBuffer[FieldValue]
```

### Decode Segment

This reads multiple columns for the same segment, following continuation
tokens.
It decodes them together into an array of `Row`s, which contain
deserialized data.

```
client.decodeSegment(
  tableName: String,
  partition: ArrayBuffer[PartitionField],
  segmentId: String,
  columnMetas: Array[ColumnMeta],
): Array[Row]
```

# Development

## Native Code

If you change any native interface code (the functions in `NativeCore.scala`),
run `sbt javah` to generate the necessary header files.
See [the JNI plugin](https://github.com/sbt/sbt-jni).

Working on a Mac, you can build the current set of pre-built binaries
by `cd`'ing into `native/` and running `sh build_all_from_darwin.sh`.
This builds the native rust code locally, then builds an Ubuntu docker image,
enters it, and builds the native rust code for x86_64 and aarch64 Linux.

+++
date = "2016-06-12T17:29:57-04:00"
title = "GridFS"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala GridFS"
weight = 90
pre = "<i class='fa'></i>"
+++


## GridFS

[GridFS]({{< docsref "core/gridfs">}}) is a specification for storing and retrieving files that exceed the BSON document size limit of 16MB. Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document.

When you query a GridFS store for a file, the Scala driver will reassemble the chunks as needed.

The following code snippets come from the [`GridFSTour.java`]({{< srcref "driver-scala/src/it/gridfs/GridFSTour.java">}}) example code
that can be found with the driver source on github.

## Prerequisites

Include the following import statements:

```scala
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import org.bson.types.ObjectId

import org.mongodb.scala._
import org.mongodb.scala.gridfs._
import org.mongodb.scala.model.Filters
import org.mongodb.scala.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream
import org.mongodb.scala.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream
```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`:

```scala
val mongoClient: MongoClient = MongoClient()
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Create a GridFS Bucket

GridFS stores files in [two collections]({{<docsref "core/gridfs/#gridfs-collections">}}): a `chunks` collection stores the file chunks, and a  `files` collection stores file metadata. The two collections are in a common bucket and the collection names are prefixed with the bucket name.

The Scala driver provides the [`GridFSBucket()`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket$.html#apply(database:org.mongodb.scala.MongoDatabase):org.mongodb.scala.gridfs.GridFSBucket" >}}) method
to create the [`GridFSBucket`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html" >}}).

```scala
val myDatabase = mongoClient.getDatabase("mydb")

// Create a gridFSBucket using the default bucket name "fs"
val gridFSBucket = GridFSBucket(myDatabase)
```

You can specify a bucket name to [`GridFSBuckets.create()`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket$.html#apply(database:org.mongodb.scala.MongoDatabase):org.mongodb.scala.gridfs.GridFSBucket">}}) method.

```scala
// Create a gridFSBucket with a custom bucket name "files"
val gridFSFilesBucket = GridFSBuckets(myDatabase, "files")
```

{{% note %}}
GridFS will automatically create indexes on the `files` and `chunks` collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Upload to GridFS

To upload data into GridFS, you can upload from an `AsyncInputStream` or write data to a `GridFSUploadStream`.

### UploadFromStream

The [`GridFSBucket.uploadFromStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#uploadFromStream(id:org.mongodb.scala.bson.BsonValue,filename:String,source:org.mongodb.scala.gridfs.AsyncInputStream,options:org.mongodb.scala.gridfs.GridFSUploadOptions):org.mongodb.scala.Observable[org.mongodb.scala.Completed]" >}}) method reads the contents of an [`AsyncInputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncInputStream.html">}}) and saves it to the `GridFSBucket`.  

You can use the [`GridFSUploadOptions`]({{< apiref "org/mongodb/scala/gridfs/index.html#GridFSUploadOptions=com.mongodb.client.gridfs.model.GridFSUploadOptions" >}}) to configure the chunk size or include additional metadata.

The following example uploads an `AsyncInputStream` into `GridFSBucket`:

```scala
// Get the input stream
val streamToUploadFrom = toAsyncInputStream(new FileInputStream(new File("/tmp/mongodb-tutorial.pdf")))

// Create some custom options
val options: GridFSUploadOptions = new GridFSUploadOptions()
    .chunkSizeBytes(358400)
    .metadata(Document("type" -> "presentation"))

val fileId: ObjectId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options).headResult()
streamToUploadFrom.close().headResult()
```

### OpenUploadStream

You can write data to a [`GridFSUploadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSUploadStream.html">}}). The [`GridFSBucket.openUploadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#openUploadStream(id:org.mongodb.scala.bson.BsonValue,filename:String):org.mongodb.scala.gridfs.GridFSUploadStream">}}) method returns a [`GridFSUploadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSUploadStream.html">}}).

The `GridFSUploadStream` buffers data until it reaches the `chunkSizeBytes` and then inserts the chunk into the `chunks` collection.  When the `GridFSUploadStream` is closed, the final chunk is written and the file metadata is inserted into the `files` collection.

The following example uploads into a `GridFSBucket` via the returned `GridFSUploadStream`:

```scala
val options: GridFSUploadOptions = new GridFSUploadOptions()
    .chunkSizeBytes(358400)
    .metadata(Document("type" -> "presentation"))

val data = Files.readAllBytes(new File("/tmp/MongoDB-manual-master.pdf").toPath)

val uploadStream: GridFSUploadStream = gridFSBucket.openUploadStream("sampleData")
uploadStream.write(data).headResult()
uploadStream.close().headResult()
println("The fileId of the uploaded file is: " + uploadStream.getObjectId().toHexString())
```

## Find Files Stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#find(filter:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.gridfs.GridFSFindObservable">}}) method.

The following example prints out the filename of each file stored:

```scala
gridFSBucket.find().results().foreach(file => println(s" - ${file.getFilename}"))
```

You can also provide a custom filter to limit the results returned. The following example prints out the filenames of all files with a "image/png" value set as the contentType in the user defined metadata document:

```scala
gridFSBucket
  .find(Filters.equal("metadata.contentType", "image/png"))
  .results()
  .foreach(file => println(s" > ${file.getFilename}"))
```

## Download from GridFS

There are various ways to download data from GridFS.

### DownloadToStream

The [`downloadToStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#downloadToStream(id:org.mongodb.scala.bson.BsonValue,destination:org.mongodb.scala.gridfs.AsyncOutputStream):org.mongodb.scala.Observable[Long]" >}}) method reads the contents from MongoDB and writes the data directly to the provided `AsyncOutputStream`.

To download a file by its file `_id`, pass the `_id` to the method. The following example downloads a file by its file `_id` into the provided
`AsyncOutputStream`:

```scala
val outputPath: Path = Paths.get("/tmp/mongodb-tutorial.pdf")
var streamToDownloadTo: AsynchronousFileChannel = AsynchronousFileChannel.open(
  outputPath,
  StandardOpenOption.CREATE_NEW,
  StandardOpenOption.WRITE,
  StandardOpenOption.DELETE_ON_CLOSE
)
gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo)).printHeadResult()
streamToDownloadTo.close()
```

If you don't know the `_id` of the file but know the filename, then you can pass the filename to the [`downloadToStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#downloadToStream(filename:String,destination:org.mongodb.scala.gridfs.AsyncOutputStream,options:org.mongodb.scala.gridfs.GridFSDownloadOptions):org.mongodb.scala.Observable[Long]" >}}) method. By default, it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "org/mongodb/scala/gridfs/model/GridFSDownloadOptions.html" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial" into the `OutputStream`:

```scala
val outputPath: Path = Paths.get("/tmp/mongodb-tutorial.pdf")

streamToDownloadTo = AsynchronousFileChannel.open(
  outputPath,
  StandardOpenOption.CREATE_NEW,
  StandardOpenOption.WRITE,
  StandardOpenOption.DELETE_ON_CLOSE
)
val downloadOptions: GridFSDownloadOptions = new GridFSDownloadOptions().revision(0)
gridFSBucket
  .downloadToStream("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions)
  .printHeadResult()
streamToDownloadTo.close()
```

### OpenDownloadStream

The [`openDownloadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#openDownloadStream(id:org.bson.types.ObjectId):org.mongodb.scala.gridfs.GridFSDownloadStream">}}) method returns a [`GridFSDownloadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSDownloadStream.html">}}) which extends [`AsyncInputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncInputStream.html">}}).

The following example reads from the `GridFSBucket` via the returned `AsyncInputStream`:

```scala
val dstByteBuffer: ByteBuffer = ByteBuffer.allocate(1024 * 1024)
val downloadStream: GridFSDownloadStream = gridFSBucket.openDownloadStream(fileId)
downloadStream
  .read(dstByteBuffer)
  .map(result => {
    dstByteBuffer.flip
    val bytes: Array[Byte] = new Array[Byte](result)
    dstByteBuffer.get(bytes)
    println(new String(bytes, StandardCharsets.UTF_8))
  })
  .headResult()
```

You can also pass the filename to the [`openDownloadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#openDownloadStream(filename:String,options:org.mongodb.scala.gridfs.GridFSDownloadOptions):org.mongodb.scala.gridfs.GridFSDownloadStream" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "org/mongodb/scala/gridfs/model/GridFSDownloadOptions.html" >}}) to configure which version to download.

The following example downloads the latest version of the file named "sampleData" into the `OutputStream`:

```scala
val downloadStreamByName: GridFSDownloadStream = gridFSBucket.openDownloadStream("sampleData")
downloadStreamByName
  .read(dstByteBuffer)
  .map(result => {
    dstByteBuffer.flip
    val bytes: Array[Byte] = new Array[Byte](result)
    dstByteBuffer.get(bytes)
    println(new String(bytes, StandardCharsets.UTF_8))
  })
  .headResult()
```

## Rename files

If you should need to rename a file, then use the [`rename`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#rename(id:org.mongodb.scala.bson.BsonValue,newFilename:String):org.mongodb.scala.Observable[org.mongodb.scala.Completed]">}}) method.  

The following example renames a file to "mongodbTutorial":

```scala
val fileId: ObjectId = ??? //ObjectId of a file uploaded to GridFS

gridFSBucket.rename(fileId, "mongodbTutorial").printResults()
```

{{% note %}}
The `rename` method requires an `ObjectId` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then for every file that should be renamed then execute `rename` with the corresponding `_id`.
{{% /note %}}

## Delete files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#delete(id:org.mongodb.scala.bson.BsonValue):org.mongodb.scala.Observable[org.mongodb.scala.Completed]">}}) method.

The following example deletes a file from the `GridFSBucket`:

```scala
val fileId: ObjectId = ??? //ObjectId of a file uploaded to GridFS

gridFSBucket.delete(fileId).printResults()
```

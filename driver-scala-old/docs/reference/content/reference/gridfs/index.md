+++
date = "2015-11-27T12:00:00-00:00"
title = "GridFS"
[menu.main]
  parent = "Reference"
  identifier = "GridFS"
  weight = 75
  pre = "<i class='fa'></i>"
+++


## GridFS

GridFS is a specification for storing and retrieving files that exceed the BSON-document size limit of 16MB.

Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document. 
By default GridFS limits chunk size to 255kb. GridFS uses two collections to store files. The chunks collection stores the file chunks, and 
the files collection stores the file metadata.

When you query a GridFS store for a file, the driver or client will reassemble the chunks as needed. GridFS is useful not only for storing 
files that exceed 16MB but also for storing any files for which you want access without having to load the entire file into memory.

{{% note %}}
For more information about GridFS see the [MongoDB GridFS documentation](http://docs.mongodb.org/manual/core/gridfs/).
{{% /note %}}

The following code snippets come from the `GridFSTour.java` example code
that can be found with the [driver source]({{< srcref "examples/src/test/scala/tour/GridFSTour.scala">}}).

{{% note class="important" %}}
This guide uses the `Helper` implicits as covered in the [Quick Tour Primer]({{< relref "getting-started/quick-tour-primer.md" >}}).
{{% /note %}}

## Async Streams

As there are multiple API's for Asynchronous I/O on the JVM the GridFS library uses a flexible interfaces for asynchronous input and output.
The [`AsyncInputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncInputStream" >}}) interface represents an `InputStream`
and the [`AsyncOutputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncOutputStream" >}}) interface represents an `OutputStream`.

In addition to these interfaces there are the following helpers:

* [`AsyncStreamHelper`]({{< apiref "org/mongodb/scala/gridfs/helpers/AsyncStreamHelper" >}}) which provides support for:
    * `byte[]`
    * `ByteBuffer`
    * `InputStream` - note: input streams are blocking
    * `OutputStream` - note: output streams are blocking
    
* [`AsynchronousChannelHelper`]({{< apiref "org/mongodb/scala/gridfs/helpers/AsynchronousChannelHelper" >}}) which provides support for:
    * `AsynchronousByteChannel`
    * `AsynchronousFileChannel`

These interfaces should be easy to wrap for any alternative asynchronous I/O implementations such as Akka, Netty or Vertx.

## Connecting to GridFS

Interactions with GridFS are done via the [`GridFSBucket`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket" >}}) class.

Creating a `GridFSBucket` requires an instance of a
[`MongoDatabase`]({{< apiref "org/mongodb/scala/MongoDatabase" >}}) and you can optionally provide a custom bucket name.

The following example shows how to create a `GridFSBucket`:

```scala
// Create a gridFSBucket using the default bucket name "fs"
val gridFSBucket: GridFSBucket = GridFSBucket(myDatabase)

// Create a gridFSBucket with a custom bucket name "files"
val customGridFSBucket: GridFSBucket = GridFSBuckets(myDatabase, "files")
```

## Uploading to GridFS

There are two main ways to upload data into GridFS.  

### UploadFromStream

The [`uploadFromStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#uploadFromStream(filename:String,source:org.mongodb.scala.gridfs.AsyncInputStream,options:org.mongodb.scala.gridfs.GridFSUploadOptions):org.mongodb.scala.Observable[org.bson.types.ObjectId]" >}}) method
reads the contents of an [`AsyncInputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncInputStream" >}}) and saves it to the `GridFSBucket`.
The size of the chunks defaults to 255 kb, but can be configured via the [`GridFSUploadOptions`]({{< apiref "org/mongodb/scala/gridfs/GridFSUploadOptions" >}}).

The following example uploads an `AsyncInputStream` into `GridFSBucket`:

```scala
// Get the input stream
val inputPath: Path = Paths.get("/tmp/mongodb-tutorial.pdf")
val streamToDownloadTo: AsynchronousFileChannel = AsynchronousFileChannel.open(inputPath, StandardOpenOptionRead)
val streamToUploadFrom: AsyncInputStream = channelToInputStream(streamToDownloadTo) // Using the AsynchronousChannelHelper

// Create some custom options
val options: GridFSUploadOptions = new GridFSUploadOptions().chunkSizeBytes(1024 * 1204).metadata(Document("type" -> "presentation"))

val fileId: ObjectId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options).headResult()
streamToUploadFrom.close()
```

### OpenUploadStream

The [`openUploadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#openUploadStream(filename:String):org.mongodb.scala.gridfs.GridFSUploadStream">}}) method returns a [`GridFSUploadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSUploadStream">}}) which extends [`AsyncOutputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncOutputStream" >}}) and can be written to.

The `GridFSUploadStream` buffers data until it reaches the `chunkSizeBytes` and then inserts the chunk into the chunks collection.
When the `GridFSUploadStream` is closed, the final chunk is written and the file metadata is inserted into the files collection.

The following example uploads an into `GridFSBucket` via the returned `OutputStream`:

```scala
val data = ByteBuffer.wrap("Data to upload into GridFS".getBytes(StandardCharsets.UTF_8))
val uploadStream: GridFSUploadStream = gridFSBucket.openUploadStream("sampleData")
uploadStream.write(data).headResult()
uploadStream.close().headResult()
```

{{% note %}}
GridFS will automatically create indexes on the files and chunks collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Finding files stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#find():org.mongodb.scala.gridfs.GridFSFindObservable">}}) method.

The following example prints out the filename of each file stored:

```scala
gridFSBucket.find().results().foreach(file => println(s" - ${file.getFilename}"))
```

You can also provide a custom filter to limit the results returned. The following example prints out the filenames of all files with a 
"image/png" value set as the contentType in the user defined metadata document:

```scala
gridFSBucket.find(Filters.equal("metadata.contentType", "image/png")).results().foreach(file => println(s" > ${file.getFilename}"))
```

## Downloading from GridFS

There are four main ways to download data from GridFS.

### DownloadToStream

The [`downloadToStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#downloadToStream(id:org.bson.types.ObjectId,destination:org.mongodb.scala.gridfs.AsyncOutputStream):org.mongodb.scala.Observable[Long]" >}}) 
method reads the contents from MongoDB and writes the data directly to the provided [`AsyncOutputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncOutputStream" >}}).

The following example downloads a file into the provided `OutputStream`:

```scala
val outputPath: Path = Paths.get("/tmp/mongodb-tutorial.txt")
var streamToDownloadTo: AsynchronousFileChannel = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE)
gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo)).headResult()
streamToDownloadTo.close()
```

### DownloadToStreamByName

If you don't know the `ObjectId` of the file you want to download, then you use the [`downloadToStreamByName`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#downloadToStream(filename:String,destination:org.mongodb.scala.gridfs.AsyncOutputStream,options:org.mongodb.scala.gridfs.GridFSDownloadOptions):org.mongodb.scala.Observable[Long]" >}}) method. 
By default it will download the latest version of the file. Use the [`GridFSDownloadByNameOptions`]({{< apiref "org/mongodb/scala/gridfs/GridFSDownloadByNameOptions" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial" into the `OutputStream`:

```scala
streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE)
val downloadOptions: GridFSDownloadOptions = new GridFSDownloadOptions().revision(0)
gridFSBucket.downloadToStream("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions).headResult()
streamToDownloadTo.close().headResult()
```

### OpenDownloadStream

The [`openDownloadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#openDownloadStream(filename:String):org.mongodb.scala.gridfs.GridFSDownloadStream">}}) 
method returns a [`GridFSDownloadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSDownloadStream">}}) which extends 
[`AsyncInputStream`]({{< apiref "org/mongodb/scala/gridfs/AsyncInputStream" >}}) and can be read from.

The following example reads from the `GridFSBucket` via the returned `AsyncInputStream`:

```scala
val dstByteBuffer: ByteBuffer = ByteBuffer.allocate(1024 * 1024)
val downloadStream: GridFSDownloadStream = gridFSBucket.openDownloadStream(fileId)
downloadStream.read(dstByteBuffer).map(result => {
  dstByteBuffer.flip
  val bytes: Array[Byte] = new Array[Byte](result)
  dstByteBuffer.get(bytes)
  println(new String(bytes, StandardCharsets.UTF_8))
}).headResult()
```

### OpenDownloadStream by name

You can also open a `GridFSDownloadStream` by searching against the filename, using the [`openDownloadStream`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#openDownloadStream(filename:String):org.mongodb.scala.gridfs.GridFSDownloadStream" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadByNameOptions`]({{< apiref "org/mongodb/scala/gridfs/GridFSDownloadByNameOptions" >}}) to configure which version to download.

The following example downloads the latest version of the file named "sampleData" into the `dstByteBuffer` ByteBuffer:

```scala
val downloadStreamByName: GridFSDownloadStream = gridFSBucket.openDownloadStream("sampleData")
downloadStreamByName.read(dstByteBuffer).map(result => {
  dstByteBuffer.flip
  val bytes: Array[Byte] = new Array[Byte](result)
  dstByteBuffer.get(bytes)
  println(new String(bytes, StandardCharsets.UTF_8))
}).headResult()
```

## Renaming files

If you should need to rename a file, then the [`rename`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#rename-org.bson.types.ObjectId-java.lang.String-">}}) method can be used.  

The following example renames a file to "mongodbTutorial":

```scala
gridFSBucket.rename(fileId, "mongodbTutorial").results()
```

{{% note %}}
The `rename` method requires an `ObjectId` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then for every file that should be renamed then execute `rename` with the corresponding `_id`.
{{% /note %}}

## Deleting files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "org/mongodb/scala/gridfs/GridFSBucket.html#delete(id:org.mongodb.scala.bson.BsonValue):org.mongodb.scala.Observable[org.mongodb.scala.Completed]">}}) method.

The following example deletes a file from the `GridFSBucket`:

```scala
gridFSBucket.delete(fileId).results()
```

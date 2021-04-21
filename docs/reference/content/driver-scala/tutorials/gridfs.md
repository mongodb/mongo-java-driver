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

[GridFS]({{< docsref "core/gridfs" >}}) is a specification for storing and retrieving files that exceed the BSON document size limit of 16MB. Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document.

When you query a GridFS store for a file, the Scala driver will reassemble the chunks as needed.

The following code snippets come from the [`GridFSTour.java`]({{< srcref "driver-scala/src/it/gridfs/GridFSTour.java" >}}) example code
that can be found with the driver source on GitHub.

## Prerequisites

Include the following import statements:

```scala
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.gridfs._
import org.mongodb.scala.model.Filters

import tour.Helpers._

import scala.util.Success
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

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Create a GridFS Bucket

GridFS stores files in [two collections]({{<docsref "core/gridfs/#gridfs-collections" >}}): a `chunks` collection stores the file chunks, and a  `files` collection stores file metadata. The two collections are in a common bucket and the collection names are prefixed with the bucket name.

The Scala driver provides the [`GridFSBucket()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket$.html#apply(database:org.mongodb.scala.MongoDatabase):org.mongodb.scala.gridfs.GridFSBucket" >}}) method
to create the [`GridFSBucket`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html" >}}).

```scala
val myDatabase = mongoClient.getDatabase("mydb")

// Create a gridFSBucket using the default bucket name "fs"
val gridFSBucket = GridFSBucket(myDatabase)
```

You can specify a bucket name to [`GridFSBuckets.create()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket$.html#apply(database:org.mongodb.scala.MongoDatabase):org.mongodb.scala.gridfs.GridFSBucket" >}}) method.

```scala
// Create a gridFSBucket with a custom bucket name "files"
val gridFSFilesBucket = GridFSBuckets(myDatabase, "files")
```

{{% note %}}
GridFS will automatically create indexes on the `files` and `chunks` collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Upload to GridFS

The [`GridFSBucket.uploadFromObservable`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html#uploadFromObservable(id:org.mongodb.scala.bson.BsonValue,filename:String,source:org.mongo.scala.Observable,options:org.mongodb.scala.gridfs.GridFSUploadOptions):org.mongodb.scala.Observable[Void]" >}}) methods read the contents of a `Observable[ByteBuffer]` and save it to the `GridFSBucket`.  

You can use the [`GridFSUploadOptions`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/index.html#GridFSUploadOptions=com.mongodb.client.gridfs.model.GridFSUploadOptions" >}}) to configure the chunk size or include additional metadata.

The following example uploads the contents of a `Observable[ByteBuffer]` into `GridFSBucket`:

```scala
// Get the input stream
val observableToUploadFrom: Observable[ByteBuffer] = Observable(
  Seq(ByteBuffer.wrap("MongoDB Tutorial..".getBytes(StandardCharsets.UTF_8)))
)

// Create some custom options
val options: GridFSUploadOptions = new GridFSUploadOptions()
    .chunkSizeBytes(358400)
    .metadata(Document("type" -> "presentation"))

val fileId: BsonObjectId = gridFSBucket.uploadFromObservable("mongodb-tutorial", observableToUploadFrom, options).headResult()
```

## Find Files Stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html#find(filter:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.gridfs.GridFSFindObservable" >}}) method.

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

The [`downloadToObservable`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html#downloadToObservable(id:org.mongodb.scala.bson.BsonValue):org.mongodb.scala.gridfs.GridFSDownloadObservable" >}}) methods return a `Observable[ByteBuffer]` that reads the contents from MongoDB.

To download a file by its file `_id`, pass the `_id` to the method. The following example downloads a file by its file `_id`:

```scala
val downloadById = gridFSBucket.downloadToObservable(fileId).results()
```

If you don't know the `_id` of the file but know the filename, then you can pass the filename to the [`downloadToObservable`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html#downloadToObservable(filename:String,options:org.mongodb.scala.gridfs.GridFSDownloadOptions):org.mongodb.scala.gridfs.GridFSDownloadObservable" >}}) method. By default, it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/model/index.html#GridFSDownloadOptions=com.mongodb.client.gridfs.model.GridFSDownloadOptions" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial":

```scala
val downloadOptions: GridFSDownloadOptions = new GridFSDownloadOptions().revision(0)
val downloadByName = gridFSBucket.downloadToObservable("mongodb-tutorial", downloadOptions).results()
```

## Rename files

If you should need to rename a file, then use the [`rename`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html#rename(id:org.mongodb.scala.bson.BsonValue,newFilename:String):org.mongodb.scala.Observable[Void]" >}}) method.  

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

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/gridfs/GridFSBucket.html#delete(id:org.mongodb.scala.bson.BsonValue):org.mongodb.scala.Observable[Void]" >}}) method.

The following example deletes a file from the `GridFSBucket`:

```scala
val fileId: ObjectId = ??? //ObjectId of a file uploaded to GridFS

gridFSBucket.delete(fileId).printResults()
```

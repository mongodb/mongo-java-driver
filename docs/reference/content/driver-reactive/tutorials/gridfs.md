+++
date = "2016-06-12T17:29:57-04:00"
title = "GridFS"
[menu.main]
parent = "Reactive Tutorials"
identifier = "Reactive GridFS"
weight = 90
pre = "<i class='fa'></i>"
+++


## GridFS

[GridFS]({{< docsref "core/gridfs" >}}) is a specification for storing and retrieving files that exceed the BSON document size limit of 16MB. Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document.

When you query a GridFS store for a file, the Java driver will reassemble the chunks as needed.

The following code snippets come from the [`GridFSTour.java`]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/gridfs/GridFSTour.java" >}}) example code
that can be found with the driver source on GitHub.

## Prerequisites

Include the following import statements:

```java
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

import com.mongodb.client.gridfs.model.*;
import com.mongodb.reactivestreams.client.gridfs.*;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.mongodb.client.model.Filters.eq;
import static reactivestreams.helpers.PublisherHelpers.toPublisher;
```

{{% note class="important" %}}
This guide uses the `Subscriber` implementations as covered in the [Quick Start Primer]({{< relref "driver-reactive/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`:

```java
MongoClient mongoClient = MongoClients.create();
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Create a GridFS Bucket

GridFS stores files in [two collections]({{<docsref "core/gridfs/#gridfs-collections" >}}): a `chunks` collection stores the file chunks, and a  `files` collection stores file metadata. The two collections are in a common bucket and the collection names are prefixed with the bucket name.

The Java driver provides the [`GridFSBuckets.create()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBuckets.html#create(com.mongodb.client.MongoDatabase)" >}}) method
to create the [`GridFSBucket`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket" >}}).

```java
MongoDatabase myDatabase = mongoClient.getDatabase("mydb");

// Create a gridFSBucket using the default bucket name "fs"
GridFSBucket gridFSBucket = GridFSBuckets.create(myDatabase);
```

You can specify a bucket name to [`GridFSBuckets.create()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBuckets.html#create(com.mongodb.client.MongoDatabase,java.lang.String)" >}}) method.

```java
// Create a gridFSBucket with a custom bucket name "files"
GridFSBucket gridFSFilesBucket = GridFSBuckets.create(myDatabase, "files");
```

{{% note %}}
GridFS will automatically create indexes on the `files` and `chunks` collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Upload to GridFS

The [`GridFSBucket.uploadFromPublisher`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket.html#uploadFromPublisher(java.lang.String,org.reactivestreams.Publisher,com.mongodb.client.gridfs.model.GridFSUploadOptions)" >}}) methods read the contents of `Publisher<ByteBuffer>` and save it to the `GridFSBucket`.  

You can use the [`GridFSUploadOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/gridfs/model/GridFSUploadOptions" >}}) to configure the chunk size or include additional metadata.

The following example uploads the contents of a `Publisher<ByteBuffer>` into `GridFSBucket`:

```java
 // Get the input publisher
Publisher<ByteBuffer> publisherToUploadFrom = toPublisher(ByteBuffer.wrap("MongoDB Tutorial..".getBytes(StandardCharsets.UTF_8)));

// Create some custom options
GridFSUploadOptions options = new GridFSUploadOptions()
        .chunkSizeBytes(1024)
        .metadata(new Document("type", "presentation"));

ObservableSubscriber<ObjectId> uploadSubscriber = new OperationSubscriber<>();
gridFSBucket.uploadFromPublisher("mongodb-tutorial", publisherToUploadFrom, options).subscribe(uploadSubscriber);
ObjectId fileId = uploadSubscriber.get().get(0);
```

## Find Files Stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket.html#find()" >}}) method.

The following example prints out the filename of each file stored:

```java
ConsumerSubscriber<GridFSFile> filesSubscriber = new ConsumerSubscriber<>(gridFSFile ->
        System.out.println(" - " + gridFSFile.getFilename()));
gridFSBucket.find().subscribe(filesSubscriber);
filesSubscriber.await();
```

You can also provide a custom filter to limit the results returned. The following example prints out the filenames of all files with a "image/png" value set as the contentType in the user defined metadata document:

```java
filesSubscriber = new ConsumerSubscriber<>(gridFSFile -> System.out.println("Found: " + gridFSFile.getFilename()));
gridFSBucket.find(eq("metadata.contentType", "image/png")).subscribe(filesSubscriber);
filesSubscriber.await();
```

## Download from GridFS

The [`downloadToPublisher`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket.html#downloadToPublisher(org.bson.types.ObjectId)" >}}) methods return a `Publisher<ByteBuffer>` that reads the contents from MongoDB.

To download a file by its file `_id`, pass the `_id` to the method. The following example downloads a file by its file `_id`:

```java
ObjectId fileId;
ObservableSubscriber<ByteBuffer> downloadSubscriber = new OperationSubscriber<>();
gridFSBucket.downloadToPublisher(fileId).subscribe(downloadSubscriber);
```

If you don't know the `_id` of the file but know the filename, then you can pass the filename to the [`downloadToPublisher`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket.html#downloadToPublisher(java.lang.String,com.mongodb.client.gridfs.model.GridFSDownloadOptions)" >}}) method. By default, it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/gridfs/model/GridFSDownloadOptions.html" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial":

```java
GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);
downloadSubscriber = new OperationSubscriber<>();
gridFSBucket.downloadToPublisher("mongodb-tutorial", downloadOptions).subscribe(downloadSubscriber);
```

## Rename files

If you should need to rename a file, then use the [`rename`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket.html#rename(org.bson.types.ObjectId,java.lang.String)" >}}) method.  

The following example renames a file to "mongodbTutorial":

```java
ObjectId fileId; //ObjectId of a file uploaded to GridFS

gridFSBucket.rename(fileId, "mongodbTutorial").subscribe(new ObservableSubscriber<Void>());
```

{{% note %}}
The `rename` method requires an `ObjectId` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then for every file that should be renamed then execute `rename` with the corresponding `_id`.
{{% /note %}}

## Delete files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/gridfs/GridFSBucket.html#delete(org.bson.types.ObjectId)" >}}) method.

The following example deletes a file from the `GridFSBucket`:

```java
ObjectId fileId; //ObjectId of a file uploaded to GridFS

gridFSBucket.delete(fileId).subscribe(new ObservableSubscriber<Void>());
```

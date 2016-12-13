+++
date = "2015-11-27T12:00:00-00:00"
title = "GridFS"
[menu.main]
  parent = "Async Tutorials"
  identifier = "Async GridFS"
  weight = 90
  pre = "<i class='fa'></i>"
+++

## GridFS

[GridFS]({{< docsref "core/gridfs">}}) is a specification for storing and retrieving files that exceed the BSON document size limit of 16MB. Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document.

When you query a GridFS store for a file, the Java Async driver will reassemble the chunks as needed.

The following code snippets come from the [`GridFSTour.java`]({{< srcref "driver-async/src/examples/gridfs/GridFSTour.java">}}) example code,
which can be found with the driver source on github.

## Async Streams

The driver's GridFS library uses flexible interfaces for asynchronous input and output. The [`AsyncInputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncInputStream" >}}) interface represents an `InputStream`
and the [`AsyncOutputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncOutputStream" >}}) interface represents an `OutputStream`.
These interfaces should be easy to wrap for any alternative asynchronous I/O implementations such as Netty or Vertx.

In addition to these interfaces, the driver provides the following helpers:

* [`AsyncStreamHelper`]({{< apiref "com/mongodb/async/client/gridfs/helpers/AsyncStreamHelper.html" >}}), which provides support for:
    * `byte[]`
    * `ByteBuffer`
    * `InputStream` - note: input streams are blocking
    * `OutputStream` - note: output streams are blocking

* [`AsynchronousChannelHelper`]({{< apiref "com/mongodb/async/client/gridfs/helpers/AsynchronousChannelHelper.html" >}}), which provides support for:
    * `AsynchronousByteChannel`
    * `AsynchronousFileChannel`


## Prerequisites

Include the following import statements:

```java
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;

import com.mongodb.async.client.gridfs.GridFSBucket;
import com.mongodb.async.client.gridfs.GridFSBuckets;
import com.mongodb.async.client.gridfs.AsyncInputStream;
import com.mongodb.async.client.gridfs.GridFSDownloadStream;
import com.mongodb.async.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream;
import static com.mongodb.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream;
import com.mongodb.async.SingleResultCallback;

import com.mongodb.Block;
import static com.mongodb.client.model.Filters.eq;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.channels.AsynchronousFileChannel;
import org.bson.Document;
import org.bson.types.ObjectId;
import java.io.FileNotFoundException;
import java.io.IOException;
```

## Consideration

{{% note class="important" %}}
Always check for errors in any [`SingleResultCallback<T>`]({{< apiref "com/mongodb/async/SingleResultCallback.html">}}) implementation
and handle them appropriately.

For sake of brevity, this tutorial omits the error check logic in the code examples.
{{% /note %}}


## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`:

```java
MongoClient mongoClient = MongoClients.create();
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< relref "driver-async/tutorials/connect-to-mongodb.md" >}}).

## Create a GridFS Bucket

GridFS stores files in [two collections]({{<docsref "core/gridfs/#gridfs-collections">}}): a `chunks` collection stores the file chunks, and a  `files` collection stores file metadata. The two collections are in a common bucket and the collection names are prefixed with the bucket name.

The Java Async driver provides the [`GridFSBuckets.create()`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBuckets.html#create-com.mongodb.async.client.MongoDatabase-" >}})  method
to create the [`GridFSBucket`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html" >}}):

```java
MongoDatabase myDatabase = mongoClient.getDatabase("mydb");

// Create a gridFSBucket using the default bucket name "fs"
GridFSBucket gridFSBucket = GridFSBuckets.create(myDatabase);
```

You can specify a bucket name to [`GridFSBuckets.create()`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBuckets.html#create-com.mongodb.async.client.MongoDatabase-" >}}) method.

```java
// Create a gridFSBucket with a custom bucket name "files"
GridFSBucket gridFSBucketCustom = GridFSBuckets.create(myDatabase, "files");
```

{{% note %}}
GridFS will automatically create indexes on the `files` and `chunks` collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Upload to GridFS

To upload data into GridFS, you can upload from an `InputStream` or write data to a `GridFSUploadStream`.

### UploadFromStream

The [`uploadFromStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openUploadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSUploadOptions-" >}}) method
reads the contents of an [`AsyncInputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncInputStream" >}}) and saves it to the `GridFSBucket`.

You can use the [`GridFSUploadOptions`]({{< apiref "com/mongodb/client/gridfs/model/GridFSUploadOptions.html" >}}) to configure the chunk size or include additional metadata.


```java
// Get the input stream
final AsyncInputStream streamToUploadFrom = toAsyncInputStream(
                    "/tmp/mongodb-tutorial.pdf".getBytes(StandardCharsets.UTF_8));

// Create some custom options
GridFSUploadOptions options = new GridFSUploadOptions()
                                    .chunkSizeBytes(1024 * 1024)
                                    .metadata(new Document("type", "presentation"));

gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options,
    new SingleResultCallback<ObjectId>() {
       @Override
       public void onResult(final ObjectId result, final Throwable t) {
           System.out.println("The fileId of the uploaded file is: " + result.toHexString());
            streamToUploadFrom.close(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    // Stream closed
                }
            });
       }
    }
);
```


### OpenUploadStream

For a finer-grained control of the upload, the driver also provides a [`GridFSBucket.openUploadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openUploadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSUploadOptions-">}}) method, which returns a [`GridFSUploadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSUploadStream.html">}}). You can write data to a [`GridFSUploadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSUploadStream.html">}}) which extends [`AsyncOutputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncOutputStream" >}}).

The [`GridFSUploadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSUploadStream.html">}}) buffers data until it reaches the `chunkSizeBytes` and then inserts the chunk into the `chunks` collection.  When the `GridFSUploadStream` is closed, the final chunk is written and the file metadata is inserted into the `files` collection.


## Find Files Stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#find--">}}) method.

The following example prints out the filename of each file stored:

```java
gridFSBucket.find().forEach(
    new Block<GridFSFile>() {
        @Override
        public void apply(final GridFSFile gridFSFile) {
            System.out.println(gridFSFile.getFilename() + " has file id: " +
                                gridFSFile.getObjectId().toHexString());
        }
    },
    new SingleResultCallback<Void>() {
        @Override
        public void onResult(final Void result, final Throwable t) {
            // Finished
        }
    }
);
```

You can also provide a custom filter to limit the results returned. The following example prints out the filenames of all files with a "image/png" value set as the contentType in the user defined metadata document:

```java
gridFSBucket.find(eq("metadata.contentType", "image/png")).forEach(
    new Block<GridFSFile>() {
        @Override
        public void apply(final GridFSFile gridFSFile) {
            System.out.println(gridFSFile.getFilename() + " has file id: " +
                               gridFSFile.getObjectId().toHexString());
        }
    },
    new SingleResultCallback<Void>() {
        @Override
        public void onResult(final Void result, final Throwable t) {
            // Finished
        }
    }
);
```

## Download from GridFS


### DownloadToStream

The [`downloadToStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#downloadToStream-org.bson.types.ObjectId-com.mongodb.async.client.gridfs.AsyncOutputStream-com.mongodb.async.SingleResultCallback-" >}}) method reads the contents from MongoDB and writes the data directly to the provided [`AsyncOutputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncOutputStream" >}}).

For the `fileId` variable, specify an `ObjectId` value returned from the find operation section in this tutorial.

```java
Path outputPath = Paths.get("/tmp/mongodb-tutorial-out.pdf");
AsynchronousFileChannel streamToDownloadTo = AsynchronousFileChannel.open(outputPath,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE,
        StandardOpenOption.DELETE_ON_CLOSE);

gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo),
    new SingleResultCallback<Long>() {
      @Override
      public void onResult(final Long result, final Throwable t) {
          System.out.println("downloaded file sized: " + result);
      }
});
// Note: AsynchronousFileChannel was opened with option delete on close
streamToDownloadTo.close();
```

If the `_id` of the file is unknown but you know the filename, then you can pass the filename to the [`downloadToStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#downloadToStream-java.lang.String-com.mongodb.async.client.gridfs.AsyncOutputStream-com.mongodb.client.gridfs.model.GridFSDownloadOptions-com.mongodb.async.SingleResultCallback-" >}}) method. By default, it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "com/mongodb/client/gridfs/model/GridFSDownloadOptions.html" >}}) to configure which version to download.

```java
Path outputPath = Paths.get("/tmp/mongodb-tutorial-out.pdf");
AsynchronousFileChannel streamToDownloadTo = AsynchronousFileChannel.open(outputPath,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE,
        StandardOpenOption.DELETE_ON_CLOSE);

GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);

gridFSBucket.downloadToStream("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions,
    new SingleResultCallback<Long>() {
      @Override
      public void onResult(final Long result, final Throwable t) {
          System.out.println("downloaded file sized: " + result);
      }
});
// Note: AsynchronousFileChannel was opened with option delete on close
streamToDownloadTo.close();
```

### OpenDownloadStream

For a finer-grained control of the upload, the driver also provides a [`openDownloadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openDownloadStream-org.bson.types.ObjectId-">}}) method, which returns a [`GridFSDownloadStream`]({{< apiref "com/mongodb/client/gridfs/GridFSDownloadStream.html">}}).

## Rename Files

If you should need to rename a file, then use the [`rename`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#rename-org.bson.types.ObjectId-java.lang.String-com.mongodb.async.SingleResultCallback-">}}) method.  

The following example renames a file to "mongodbTutorial":

```java
ObjectId fileId; // The id of a file uploaded to GridFS, initialize to valid file id 
...
gridFSBucket.rename(fileId, "mongodbTutorial", new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Renamed file");
    }
});
```

{{% note %}}
The `rename` method requires an `_id` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then, for every file that should be renamed, execute `rename` with the corresponding `_id`.
{{% /note %}}

## Delete Files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#delete-org.bson.types.ObjectId-com.mongodb.async.SingleResultCallback-">}}) method.

The following example deletes a file from the `GridFSBucket`:

```java
ObjectId fileId; // The id of a file uploaded to GridFS, initialize to valid file id 
...
gridFSBucket.delete(fileId, new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Deleted file");
    }
});
```

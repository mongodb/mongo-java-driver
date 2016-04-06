+++
date = "2015-11-27T12:00:00-00:00"
title = "GridFS"
[menu.main]
  parent = "Async Reference"
  identifier = "Async GridFS"
  weight = 80
  pre = "<i class='fa'></i>"
+++


## GridFS

GridFS is a specification for storing and retrieving files that exceed the BSON-document size limit of 16MB.

Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document. By default GridFS limits chunk size to 255k. GridFS uses two collections to store files. The chunks collection stores the file chunks, and the files collection stores the file metadata.

When you query a GridFS store for a file, the driver or client will reassemble the chunks as needed. GridFS is useful not only for storing files that exceed 16MB but also for storing any files for which you want access without having to load the entire file into memory.

{{% note %}}
For more information about GridFS see the [MongoDB GridFS documentation](http://docs.mongodb.org/manual/core/gridfs/).
{{% /note %}}

The following code snippets come from the `GridFSTour.java` example code
that can be found with the [driver source]({{< srcref "driver-async/src/examples/gridfs/GridFSTour.java">}}).

{{% note class="important" %}}
It's important to always check for errors in any `SingleResponseCallback<T>` implementation and handle them appropriately! 
Below the error checks are left out only for the sake of brevity.
{{% /note %}}

## Async Streams

As there are multiple API's for Asynchronous I/O on the JVM the GridFS library uses a flexible interfaces for asynchronous input and output.
The [`AsyncInputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncInputStream" >}}) interface represents an `InputStream`
and the [`AsyncOutputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncOutputStream" >}}) interface represents an `OutputStream`.

In addition to these interfaces there are the following helpers:

* [`AsyncStreamHelper`]({{< apiref "com/mongodb/async/client/gridfs/helpers/AsyncStreamHelper" >}}) which provides support for:
    * `byte[]`
    * `ByteBuffer`
    * `InputStream` - note: input streams are blocking
    * `OutputStream` - note: output streams are blocking
    
* [`AsynchronousChannelHelper`]({{< apiref "com/mongodb/async/client/gridfs/helpers/AsynchronousChannelHelper" >}}) which provides support for:
    * `AsynchronousByteChannel`
    * `AsynchronousFileChannel`

These interfaces should be easy to wrap for any alternative asynchronous I/O implementations such as Netty or Vertx.

## Connecting to GridFS

Interactions with GridFS are done via the [`GridFSBucket`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket" >}}) class.
To create a `GridFSBucket` use the [`GridFSBuckets`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBuckets" >}}) factory class.  

Creating a `GridFSBucket` requires an instance of a
[`MongoDatabase`]({{< apiref "com/mongodb/async/client/MongoDatabase" >}}) and you can optionally provide a custom bucket name.

The following example shows how to create a `GridFSBucket`:

```java
// Create a gridFSBucket using the default bucket name "fs"
GridFSBucket gridFSBucket = GridFSBuckets.create(myDatabase);

// Create a gridFSBucket with a custom bucket name "files"
GridFSBucket gridFSBucket = GridFSBuckets.create(myDatabase, "files");
```

## Uploading to GridFS

There are two main ways to upload data into GridFS.  

### UploadFromStream

The [`uploadFromStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openUploadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSUploadOptions-" >}}) method
reads the contents of an [`AsyncInputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncInputStream" >}}) and saves it to the `GridFSBucket`.
The size of the chunks defaults to 255 bytes, but can be configured via the [`GridFSUploadOptions`]({{< apiref "com/mongodb/async/client/gridfs/model/GridFSUploadOptions" >}}).

The following example uploads an `AsyncInputStream` into `GridFSBucket`:

```java
// Get the input stream
Path inputPath = Paths.get("/tmp/mongodb-tutorial.pdf");
AsynchronousFileChannel streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOptionRead);
final AsyncInputStream streamToUploadFrom = channelToInputStream(streamToDownloadTo); // Using the AsynchronousChannelHelper

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

The [`openUploadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openUploadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSUploadOptions-">}}) method returns a [`GridFSUploadStream`]({{< apiref "mongodb/client/gridfs/GridFSUploadStream.html">}}) which extends [`AsyncOutputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncOutputStream" >}}) and can be written to.

The `GridFSUploadStream` buffers data until it reaches the `chunkSizeBytes` and then inserts the chunk into the chunks collection.
When the `GridFSUploadStream` is closed, the final chunk is written and the file metadata is inserted into the files collection.

The following example uploads an into `GridFSBucket` via the returned `OutputStream`:

```java
ByteBuffer data = ByteBuffer.wrap("Data to upload into GridFS".getBytes(StandardCharsets.UTF_8));
final GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("sampleData");
uploadStream.write(data, new SingleResultCallback<Integer>() {
    @Override
    public void onResult(final Integer result, final Throwable t) {
        System.out.println("The fileId of the uploaded file is: " + uploadStream.getFileId().toHexString());

        uploadStream.close(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Stream closed
            }
        });
    }
});
```

{{% note %}}
GridFS will automatically create indexes on the files and chunks collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Finding files stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#find--">}}) method.

The following example prints out the filename of each file stored:

```java
gridFSBucket.find().forEach(
    new Block<GridFSFile>() {
        @Override
        public void apply(final GridFSFile gridFSFile) {
            System.out.println(gridFSFile.getFilename());
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
            System.out.println(gridFSFile.getFilename());
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

## Downloading from GridFS

There are four main ways to download data from GridFS.

### DownloadFromStream

The [`downloadToStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#downloadToStream-org.bson.types.ObjectId-java.io.OutputStream-" >}}) method reads the contents from MongoDB and writes the data directly to the provided [`AsyncOutputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncOutputStream" >}}).

The following example downloads a file into the provided `OutputStream`:

```java
Path outputPath = Paths.get("/tmp/mongodb-tutorial.pdf");
final AsynchronousFileChannel streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo), new SingleResultCallback<Long>() {
    @Override
    public void onResult(final Long result, final Throwable t) {
        streamToDownloadTo.close();
        System.out.println("downloaded file sized: " + result);
    }
});
```

### DownloadToStreamByName

If you don't know the [`ObjectId`]({{< apiref "org/bson/types/ObjectId.html">}}) of the file you want to download, then you use the [`downloadToStreamByName`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#downloadToStreamByName-java.lang.String-java.io.OutputStream-com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions-" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadByNameOptions`]({{< apiref "com/mongodb/async/client/gridfs/model/GridFSDownloadByNameOptions.html" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial" into the `OutputStream`:

```java
final streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
        StandardOpenOption.DELETE_ON_CLOSE);
GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(0);
gridFSBucket.downloadToStreamByName("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions,
    new SingleResultCallback<Long>() {
        @Override
        public void onResult(final Long result, final Throwable t) {
            System.out.println("downloaded file sized: " + result);
            streamToDownloadTo.close();
        }
    }
);
```

### OpenDownloadStream

The [`openDownloadStream`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openDownloadStream-org.bson.types.ObjectId-">}}) method returns a [`GridFSDownloadStream`]({{< apiref "mongodb/client/gridfs/GridFSDownloadStream.html">}}) which extends [`AsyncInputStream`]({{< apiref "com/mongodb/async/client/gridfs/AsyncInputStream" >}}) and can be read from.

The following example reads from the `GridFSBucket` via the returned `AsyncInputStream`:

```java
final ByteBuffer dstByteBuffer = ByteBuffer.allocate(1024 * 1024);
final GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(fileId);
downloadStream.read(dstByteBuffer, new SingleResultCallback<Integer>() {
    @Override
    public void onResult(final Integer result, final Throwable t) {
        dstByteBuffer.flip();
        byte[] bytes = new byte[result];
        dstByteBuffer.get(bytes);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));

        downloadStream.close(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Finished
            }
        });
    }
});
```

### OpenDownloadStreamByName

You can also open a `GridFSDownloadStream` by searching against the filename, using the [`openDownloadStreamByName`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#openDownloadStreamByName-java.lang.String-com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions-" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadByNameOptions`]({{< apiref "com/mongodb/async/client/gridfs/model/GridFSDownloadByNameOptions.html" >}}) to configure which version to download.

The following example downloads the latest version of the file named "sampleData" into the `dstByteBuffer` ByteBuffer:

```java
final GridFSDownloadStream downloadStreamByName = gridFSBucket.openDownloadStreamByName("sampleData");
final ByteBuffer dstByteBuffer = ByteBuffer.allocate(1024 * 1024);
downloadStreamByName.read(dstByteBuffer, new SingleResultCallback<Integer>() {
    @Override
    public void onResult(final Integer result, final Throwable t) {
        dstByteBuffer.flip();
        byte[] bytes = new byte[result];
        dstByteBuffer.get(bytes);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));

        downloadStreamByName.close(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Finished
            }
        });
    }
});
```

## Renaming files

If you should need to rename a file, then the [`rename`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#rename-org.bson.types.ObjectId-java.lang.String-">}}) method can be used.  

The following example renames a file to "mongodbTutorial":

```java
gridFSBucket.rename(fileId, "mongodbTutorial", new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Renamed file");
    }
});
```

{{% note %}}
The `rename` method requires an `ObjectId` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then for every file that should be renamed then execute `rename` with the corresponding `_id`.
{{% /note %}}

## Deleting files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "com/mongodb/async/client/gridfs/GridFSBucket.html#delete-org.bson.types.ObjectId-">}}) method.

The following example deletes a file from the `GridFSBucket`:

```java
gridFSBucket.delete(fileId, new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Deleted file");
    }
});
```

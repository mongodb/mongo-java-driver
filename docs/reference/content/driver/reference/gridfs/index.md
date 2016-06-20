+++
date = "2015-08-05T12:00:00-00:00"
title = "GridFS"
[menu.main]
  parent = "Sync Reference"
  identifier = "Sync GridFS"
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
that can be found with the [driver source]({{< srcref "driver/src/examples/gridfs/GridFSTour.java">}}).

## Connecting to GridFS

Interactions with GridFS are done via the [`GridFSBucket`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket" >}}) class. To create a `GridFSBucket` use the [`GridFSBuckets`]({{< apiref "com/mongodb/client/gridfs/GridFSBuckets" >}}) factory class.  

Creating a `GridFSBucket` requires an instance of a
[`MongoDatabase`]({{< apiref "com/mongodb/client/MongoDatabase" >}}) and you can optionally provide a custom bucket name.

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

The [`uploadFromStream`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#openUploadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSUploadOptions-" >}}) method reads the contents of an [`InputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/InputStream.html) and saves it to the `GridFSBucket`.  The size of the chunks defaults to 255 bytes, but can be configured via the [`GridFSUploadOptions`]({{< apiref "com/mongodb/client/gridfs/model/GridFSUploadOptions" >}}).

The following example uploads an `InputStream` into `GridFSBucket`:

```java
// Get the input stream
InputStream streamToUploadFrom = new FileInputStream(new File("/tmp/mongodb-tutorial.pdf"));

// Create some custom options
GridFSUploadOptions options = new GridFSUploadOptions()
                                    .chunkSizeBytes(1024)
                                    .metadata(new Document("type", "presentation"));

ObjectId fileId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options);
```

### OpenUploadStream

The [`openUploadStream`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#openUploadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSUploadOptions-">}}) method returns a [`GridFSUploadStream`]({{< apiref "mongodb/client/gridfs/GridFSUploadStream.html">}}) which extends [`OutputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/OutputStream.html) and can be written to.

The `GridFSUploadStream` buffers data until it reaches the `chunkSizeBytes` and then inserts the chunk into the chunks collection.  When the `GridFSUploadStream` is closed, the final chunk is written and the file metadata is inserted into the files collection.

The following example uploads an into `GridFSBucket` via the returned `OutputStream`:

```java
byte[] data = "Data to upload into GridFS".getBytes(StandardCharsets.UTF_8);
GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("sampleData", options);
uploadStream.write(data);
uploadStream.close();
System.out.println("The fileId of the uploaded file is: " + uploadStream.getFileId().toHexString());
```

{{% note %}}
GridFS will automatically create indexes on the files and chunks collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Finding files stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#find--">}}) method.

The following example prints out the filename of each file stored:

```java
gridFSBucket.find().forEach(
  new Block<GridFSFile>() {
    @Override
    public void apply(final GridFSFile gridFSFile) {
        System.out.println(gridFSFile.getFilename());
    }
});
```

You can also provide a custom filter to limit the results returned. The following example prints out the filenames of all files with a "image/png" value set as the contentType in the user defined metadata document:

```java
gridFSBucket.find(eq("metadata.contentType", "image/png")).forEach(
  new Block<GridFSFile>() {
      @Override
      public void apply(final GridFSFile gridFSFile) {
          System.out.println(gridFSFile.getFilename());
      }
  });
```

## Downloading from GridFS

There are four main ways to download data from GridFS.

### DownloadFromStream

The [`downloadToStream`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#downloadToStream-org.bson.types.ObjectId-java.io.OutputStream-" >}}) method reads the contents from MongoDB and writes the data directly to the provided [`OutputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/OutputStream.html).

The following example downloads a file into the provided `OutputStream`:

```java
FileOutputStream streamToDownloadTo = new FileOutputStream("/tmp/mongodb-tutorial.pdf");
gridFSBucket.downloadToStream(fileId, streamToDownloadTo);
streamToDownloadTo.close();
System.out.println(streamToDownloadTo.toString());
```

If you don't know the [`ObjectId`]({{< apiref "org/bson/types/ObjectId.html">}}) of the file but know the filename, you can use the [`downloadToStream`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#downloadToStream-java.lang.String-java.io.OutputStream-com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions-" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadByNameOptions`]({{< apiref "com/mongodb/client/gridfs/model/GridFSDownloadByNameOptions.html" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial" into the `OutputStream`:

```java
FileOutputStream streamToDownloadTo = new FileOutputStream("/tmp/mongodb-tutorial.pdf");
GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(0);
gridFSBucket.downloadToStream("mongodb-tutorial", streamToDownloadTo, downloadOptions);
streamToDownloadTo.close();
```

### OpenDownloadStream

The [`openDownloadStream`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#openDownloadStream-org.bson.types.ObjectId-">}}) method returns a [`GridFSDownloadStream`]({{< apiref "mongodb/client/gridfs/GridFSDownloadStream.html">}}) which extends [`InputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/InputStream.html) and can be read from.

The following example reads from the `GridFSBucket` via the returned `InputStream`:

```java
GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(fileId);
int fileLength = (int) downloadStream.getGridFSFile().getLength();
byte[] bytesToWriteTo = new byte[fileLength];
downloadStream.read(bytesToWriteTo);
downloadStream.close();

System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));
```

You can also open a `GridFSDownloadStream` by searching against the filename, using the [`openDownloadStream`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#openDownloadStream-java.lang.String-com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions-" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadByNameOptions`]({{< apiref "com/mongodb/client/gridfs/model/GridFSDownloadByNameOptions.html" >}}) to configure which version to download.

The following example downloads the latest version of the file named "sampleData" into the `OutputStream`:

```java
GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream("sampleData");
int fileLength = (int) downloadStream.getGridFSFile().getLength();
byte[] bytesToWriteTo = new byte[fileLength];
downloadStream.read(bytesToWriteTo);
downloadStream.close();

System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));
```

## Renaming files

If you should need to rename a file, then the [`rename`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#rename-org.bson.types.ObjectId-java.lang.String-">}}) method can be used.  

The following example renames a file to "mongodbTutorial":

```java
gridFSBucket.rename(fileId, "mongodbTutorial");
```

{{% note %}}
The `rename` method requires an `ObjectId` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then for every file that should be renamed then execute `rename` with the corresponding `_id`.
{{% /note %}}

## Deleting files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "com/mongodb/client/gridfs/GridFSBucket.html#delete-org.bson.types.ObjectId-">}}) method.

The following example deletes a file from the `GridFSBucket`:

```java
gridFSBucket.delete(fileId);
```

+++
date = "2016-06-12T17:29:57-04:00"
title = "GridFS"
[menu.main]
parent = "Sync Tutorials"
identifier = "GridFS"
weight = 90
pre = "<i class='fa'></i>"
+++


## GridFS

[GridFS]({{< docsref "core/gridfs" >}}) is a specification for storing and retrieving files that exceed the BSON document size limit of 16MB. Instead of storing a file in a single document, GridFS divides a file into parts, or chunks, and stores each of those chunks as a separate document.

When you query a GridFS store for a file, the Java driver will reassemble the chunks as needed.

The following code snippets come from the [`GridFSTour.java`]({{< srcref "driver-sync/src/examples/gridfs/GridFSTour.java" >}}) example code
that can be found with the driver source on github.

## Prerequisites

Include the following import statements:

```java
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.model.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import java.io.*;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import static com.mongodb.client.model.Filters.eq;
```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`:

```java
MongoClient mongoClient = MongoClients.create();
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Create a GridFS Bucket

GridFS stores files in [two collections]({{<docsref "core/gridfs/#gridfs-collections" >}}): a `chunks` collection stores the file chunks, and a  `files` collection stores file metadata. The two collections are in a common bucket and the collection names are prefixed with the bucket name.

The Java driver provides the [`GridFSBuckets.create()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBuckets.html#create(com.mongodb.client.MongoDatabase)" >}}) method
to create the [`GridFSBucket`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket" >}}).

```java
MongoDatabase myDatabase = mongoClient.getDatabase("mydb");

// Create a gridFSBucket using the default bucket name "fs"
GridFSBucket gridFSBucket = GridFSBuckets.create(myDatabase);
```

You can specify a bucket name to [`GridFSBuckets.create()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBuckets.html#create(com.mongodb.client.MongoDatabase,java.lang.String)" >}}) method.

```java
// Create a gridFSBucket with a custom bucket name "files"
GridFSBucket gridFSFilesBucket = GridFSBuckets.create(myDatabase, "files");
```

{{% note %}}
GridFS will automatically create indexes on the `files` and `chunks` collections on first upload of data into the GridFS bucket.
{{% /note %}}

## Upload to GridFS

To upload data into GridFS, you can upload from an `InputStream` or write data to a `GridFSUploadStream`.

### UploadFromStream

The [`GridFSBucket.uploadFromStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#openUploadStream(java.lang.String,com.mongodb.client.gridfs.model.GridFSUploadOptions)" >}}) method reads the contents of an [`InputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/InputStream.html) and saves it to the `GridFSBucket`.  

You can use the [`GridFSUploadOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/gridfs/model/GridFSUploadOptions" >}}) to configure the chunk size or include additional metadata.

The following example uploads an `InputStream` into `GridFSBucket`:

```java
// Get the input stream

try {
    InputStream streamToUploadFrom = new FileInputStream(new File("/tmp/mongodb-tutorial.pdf"));
    // Create some custom options
    GridFSUploadOptions options = new GridFSUploadOptions()
                                        .chunkSizeBytes(358400)
                                        .metadata(new Document("type", "presentation"));

    ObjectId fileId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options);
} catch (FileNotFoundException e){
   // handle exception
}
```

### OpenUploadStream

You can write data to a [`GridFSUploadStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSUploadStream.html" >}}) which extends
 [`OutputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/OutputStream.html). The 
 [`GridFSBucket.openUploadStream `]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#openUploadStream(java.lang.String,com.mongodb.client.gridfs.model.GridFSUploadOptions)" >}}) 
 method returns a [`GridFSUploadStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSUploadStream.html" >}}).

The `GridFSUploadStream` buffers data until it reaches the `chunkSizeBytes` and then inserts the chunk into the `chunks` collection.  When the `GridFSUploadStream` is closed, the final chunk is written and the file metadata is inserted into the `files` collection.

The following example uploads into a `GridFSBucket` via the returned `GridFSUploadStream`:

```java
try {
    GridFSUploadOptions options = new GridFSUploadOptions()
                       .chunkSizeBytes(358400)
                       .metadata(new Document("type", "presentation"));

    GridFSUploadStream uploadStream = gridFSFilesBucket.openUploadStream("mongodb-tutorial-2", options);
    byte[] data = Files.readAllBytes(new File("/tmp/MongoDB-manual-master.pdf").toPath());

    uploadStream.write(data);
    uploadStream.close();
    System.out.println("The fileId of the uploaded file is: " + uploadStream.getObjectId().toHexString());

} catch(IOException e){
    // handle exception
}
```

## Find Files Stored in GridFS

To find the files stored in the `GridFSBucket` use the [`find`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#find()" >}}) method.

The following example prints out the filename of each file stored:

```java
gridFSBucket.find()
        .forEach(gridFSFile -> System.out.println(gridFSFile.getFilename()));
```

You can also provide a custom filter to limit the results returned. The following example prints out the filenames of all files with a "image/png" value set as the contentType in the user defined metadata document:

```java
gridFSBucket.find(eq("metadata.contentType", "image/png"))
        .forEach(gridFSFile -> System.out.println(gridFSFile.getFilename()));
```

## Download from GridFS

There are various ways to download data from GridFS.

### DownloadToStream

The [`downloadToStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#downloadToStream(org.bson.types.ObjectId,java.io.OutputStream)" >}}) method reads the contents from MongoDB and writes the data directly to the provided [`OutputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/OutputStream.html).

To download a file by its file `_id`, pass the `_id` to the method. The
following example downloads a file by its file `_id` into the provided
`OutputStream`:

```java
ObjectId fileId; //The id of a file uploaded to GridFS, initialize to valid file id 

try {
    FileOutputStream streamToDownloadTo = new FileOutputStream("/tmp/mongodb-tutorial.pdf");
    gridFSBucket.downloadToStream(fileId, streamToDownloadTo);
    streamToDownloadTo.close();
    System.out.println(streamToDownloadTo.toString());
} catch (IOException e) {
    // handle exception
}

```

If you don't know the `_id` of the file but know the filename, then you can pass the filename to the [`downloadToStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#downloadToStream(java.lang.String,java.io.OutputStream,com.mongodb.client.gridfs.model.GridFSDownloadOptions)" >}}) method. By default, it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/gridfs/model/GridFSDownloadOptions.html" >}}) to configure which version to download.

The following example downloads the original version of the file named "mongodb-tutorial" into the `OutputStream`:

```java
try {
    FileOutputStream streamToDownloadTo = new FileOutputStream("/tmp/mongodb-tutorial.pdf");
    GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);
    gridFSBucket.downloadToStream("mongodb-tutorial", streamToDownloadTo, downloadOptions);
    streamToDownloadTo.close();
} catch (IOException e) {
   // handle exception
}

```

### OpenDownloadStream

 
The [`openDownloadStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#openDownloadStream(org.bson.types.ObjectId)" >}}) method returns a [`GridFSDownloadStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSDownloadStream.html" >}}) which extends [`InputStream`](http://docs.oracle.com/javase/8/docs/api/index.html?java/io/InputStream.html).

The following example reads from the `GridFSBucket` via the returned `InputStream`:

```java
ObjectId fileId; //The id of a file uploaded to GridFS, initialize to valid file id 

GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(fileId);
int fileLength = (int) downloadStream.getGridFSFile().getLength();
byte[] bytesToWriteTo = new byte[fileLength];
downloadStream.read(bytesToWriteTo);
downloadStream.close();

System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));
```

You can also pass the filename to the [`openDownloadStream`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#openDownloadStream(java.lang.String,com.mongodb.client.gridfs.model.GridFSDownloadOptions)" >}}) method. By default it will download the latest version of the file. Use the [`GridFSDownloadOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/gridfs/model/GridFSDownloadOptions.html" >}}) to configure which version to download.

The following example downloads the latest version of the file named "sampleData" into the `OutputStream`:

```java
GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream("sampleData");
int fileLength = (int) downloadStream.getGridFSFile().getLength();
byte[] bytesToWriteTo = new byte[fileLength];
downloadStream.read(bytesToWriteTo);
downloadStream.close();

System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));
```

## Rename files

If you should need to rename a file, then use the [`rename`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#rename(org.bson.types.ObjectId,java.lang.String)" >}}) method.  

The following example renames a file to "mongodbTutorial":

```java
ObjectId fileId; //ObjectId of a file uploaded to GridFS

gridFSBucket.rename(fileId, "mongodbTutorial");
```

{{% note %}}
The `rename` method requires an `ObjectId` rather than a `filename` to ensure the correct file is renamed.

To rename multiple revisions of the same filename, first retrieve the full list of files. Then for every file that should be renamed then execute `rename` with the corresponding `_id`.
{{% /note %}}

## Delete files

To delete a file from the `GridFSBucket` use the [`delete`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/gridfs/GridFSBucket.html#delete(org.bson.types.ObjectId)" >}}) method.

The following example deletes a file from the `GridFSBucket`:

```java
ObjectId fileId; //ObjectId of a file uploaded to GridFS

gridFSBucket.delete(fileId);
```

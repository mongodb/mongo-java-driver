/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package reactivestreams.gridfs;


import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream;
import org.bson.Document;
import org.bson.types.ObjectId;
import reactivestreams.helpers.SubscriberHelpers.ConsumerSubscriber;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.reactivestreams.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream;
import static com.mongodb.reactivestreams.client.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream;

/**
 * The GridFS code example see: https://mongodb.github.io/mongo-java-driver/3.1/driver/reference/gridfs
 */
public final class GridFSTour {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     * @throws FileNotFoundException if the sample file cannot be found
     * @throws IOException if there was an exception closing an input stream
     */
    public static void main(final String[] args) throws FileNotFoundException, IOException {
        MongoClient mongoClient;

        if (args.length == 0) {
            // connect to the local database server
            mongoClient = MongoClients.create();
        } else {
            mongoClient = MongoClients.create(args[0]);
        }

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb");
        ObservableSubscriber<Success> dropSubscriber = new OperationSubscriber<>();
        database.drop().subscribe(dropSubscriber);
        dropSubscriber.await();

        GridFSBucket gridFSBucket = GridFSBuckets.create(database);

        /*
         * UploadFromStream Example
         */
        // Get the input stream
        final AsyncInputStream streamToUploadFrom = toAsyncInputStream("MongoDB Tutorial..".getBytes(StandardCharsets.UTF_8));

        // Create some custom options
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(1024)
                .metadata(new Document("type", "presentation"));

        ObservableSubscriber<ObjectId> uploadSubscriber = new OperationSubscriber<>();
        gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options).subscribe(uploadSubscriber);
        ObjectId fileId = uploadSubscriber.get().get(0);

        /*
         * OpenUploadStream Example
         */

        // Get some data to write
        ByteBuffer data = ByteBuffer.wrap("Data to upload into GridFS".getBytes(StandardCharsets.UTF_8));

        final GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("sampleData");
        ObservableSubscriber<Integer> writeSubscriber = new OperationSubscriber<>();
        ObservableSubscriber<Success> closedStreamSubscriber = new OperationSubscriber<>();
        uploadStream.write(data).subscribe(writeSubscriber);
        writeSubscriber.await();
        uploadStream.close().subscribe(closedStreamSubscriber);
        closedStreamSubscriber.await();
        System.out.println("The fileId of the uploaded file is: " + uploadStream.getObjectId().toHexString());

        /*
         * Find documents
         */
        System.out.println("File names:");
        ConsumerSubscriber<GridFSFile> filesSubscriber = new ConsumerSubscriber<>(gridFSFile ->
                System.out.println(" - " + gridFSFile.getFilename()));
        gridFSBucket.find().subscribe(filesSubscriber);
        filesSubscriber.await();

        /*
         * Find documents with a filter
         */
        filesSubscriber = new ConsumerSubscriber<>(gridFSFile -> System.out.println("Found: " + gridFSFile.getFilename()));
        gridFSBucket.find(eq("metadata.contentType", "image/png")).subscribe(filesSubscriber);
        filesSubscriber.await();

        /*
         * DownloadToStream
         */
        Path outputPath = Paths.get("/tmp/mongodb-tutorial.txt");
        AsynchronousFileChannel streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
        ConsumerSubscriber<Long> downloadSubscriber = new ConsumerSubscriber<>(result ->
                System.out.println("downloaded file sized: " + result));
        gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo)).subscribe(downloadSubscriber);
        downloadSubscriber.await();
        streamToDownloadTo.close();

        /*
         * DownloadToStreamByName
         */
        streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                StandardOpenOption.DELETE_ON_CLOSE);
        GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);
        downloadSubscriber = new ConsumerSubscriber<>(result -> System.out.println("downloaded file sized: " + result));
        gridFSBucket.downloadToStream("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions)
                .subscribe(downloadSubscriber);
        downloadSubscriber.await();
        streamToDownloadTo.close();

        /*
         * OpenDownloadStream
         */
        final ByteBuffer dstByteBuffer = ByteBuffer.allocate(1024 * 1024);
        final GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(fileId);
        ObservableSubscriber<Integer> fileDownloadSubscriber = new ConsumerSubscriber<>(fileSize -> {
            dstByteBuffer.flip();
            byte[] bytes = new byte[fileSize];
            dstByteBuffer.get(bytes);
            System.out.println(" File contents: " + new String(bytes, StandardCharsets.UTF_8));
            dstByteBuffer.clear();
        });
        downloadStream.read(dstByteBuffer).subscribe(fileDownloadSubscriber);
        fileDownloadSubscriber.await();

        /*
         * OpenDownloadStreamByName
         */
        System.out.println("ByName");

        final GridFSDownloadStream downloadStreamByName = gridFSBucket.openDownloadStream("sampleData");
        fileDownloadSubscriber = new ConsumerSubscriber<>(fileSize -> {
            dstByteBuffer.flip();
            byte[] bytes = new byte[fileSize];
            dstByteBuffer.get(bytes);
            System.out.println(" File contents: " + new String(bytes, StandardCharsets.UTF_8));
            dstByteBuffer.clear();
        });
        downloadStreamByName.read(dstByteBuffer).subscribe(fileDownloadSubscriber);
        fileDownloadSubscriber.await();

        /*
         * Rename
         */
        OperationSubscriber<Success> successSubscriber = new OperationSubscriber<>();
        gridFSBucket.rename(fileId, "mongodbTutorial").subscribe(successSubscriber);
        successSubscriber.await();
        System.out.println("Renamed file");

        /*
         * Delete
         */
        successSubscriber = new OperationSubscriber<>();
        gridFSBucket.delete(fileId).subscribe(successSubscriber);
        successSubscriber.await();
        System.out.println("Deleted file");

        // Final cleanup
        successSubscriber = new OperationSubscriber<>();
        database.drop().subscribe(successSubscriber);
        successSubscriber.await();
        System.out.println("Finished");
    }

    private GridFSTour() {
    }
}

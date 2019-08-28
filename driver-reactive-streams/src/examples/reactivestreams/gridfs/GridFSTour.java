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

import com.mongodb.Block;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.MongoClient;
import com.mongodb.internal.async.client.MongoClients;
import com.mongodb.internal.async.client.MongoDatabase;
import com.mongodb.internal.async.client.gridfs.AsyncInputStream;
import com.mongodb.internal.async.client.gridfs.GridFSBucket;
import com.mongodb.internal.async.client.gridfs.GridFSBuckets;
import com.mongodb.internal.async.client.gridfs.GridFSDownloadStream;
import com.mongodb.internal.async.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.internal.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream;
import static com.mongodb.internal.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream;
import static com.mongodb.client.model.Filters.eq;

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
     * @throws InterruptedException if a latch was interrupted
     */
    public static void main(final String[] args) throws FileNotFoundException, InterruptedException, IOException {
        MongoClient mongoClient;

        if (args.length == 0) {
            // connect to the local database server
            mongoClient = MongoClients.create();
        } else {
            mongoClient = MongoClients.create(args[0]);
        }

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb");
        final CountDownLatch dropLatch = new CountDownLatch(1);
        database.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch.countDown();
            }
        });
        dropLatch.await();

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

        final AtomicReference<ObjectId> fileIdRef = new AtomicReference<ObjectId>();
        final CountDownLatch uploadLatch = new CountDownLatch(1);
        gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options, new SingleResultCallback<ObjectId>() {
            @Override
            public void onResult(final ObjectId result, final Throwable t) {
                fileIdRef.set(result);
                System.out.println("The fileId of the uploaded file is: " + result.toHexString());
                streamToUploadFrom.close(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        uploadLatch.countDown();
                    }
                });
            }
        });
        uploadLatch.await();

        ObjectId fileId = fileIdRef.get();

        /*
         * OpenUploadStream Example
         */

        // Get some data to write
        ByteBuffer data = ByteBuffer.wrap("Data to upload into GridFS".getBytes(StandardCharsets.UTF_8));

        final CountDownLatch uploadLatch2 = new CountDownLatch(2);
        final GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("sampleData");
        uploadStream.write(data, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final Throwable t) {
                uploadLatch2.countDown();
                System.out.println("The fileId of the uploaded file is: " + uploadStream.getObjectId().toHexString());

                uploadStream.close(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        uploadLatch2.countDown();
                    }
                });
            }
        });

        uploadLatch2.await();

        /*
         * Find documents
         */
        final CountDownLatch findLatch = new CountDownLatch(1);
        System.out.println("File names:");
        gridFSBucket.find().forEach(new Block<GridFSFile>() {
            @Override
            public void apply(final GridFSFile gridFSFile) {
                System.out.println(" - " + gridFSFile.getFilename());
            }
        }, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                findLatch.countDown();
            }
        });
        findLatch.await();

        /*
         * Find documents with a filter
         */
        final CountDownLatch findLatch2 = new CountDownLatch(1);
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
                        findLatch2.countDown();
                    }
         });
        findLatch2.await();

        /*
         * DownloadToStream
         */
        Path outputPath = Paths.get("/tmp/mongodb-tutorial.txt");
        AsynchronousFileChannel streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
        final CountDownLatch downloadLatch = new CountDownLatch(1);
        gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo), new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                downloadLatch.countDown();
                System.out.println("downloaded file sized: " + result);
            }
        });
        downloadLatch.await();
        streamToDownloadTo.close();

        /*
         * DownloadToStreamByName
         */
        final CountDownLatch downloadLatch2 = new CountDownLatch(1);
        streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                StandardOpenOption.DELETE_ON_CLOSE);
        GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);
        gridFSBucket.downloadToStream("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions,
                new SingleResultCallback<Long>() {
                    @Override
                    public void onResult(final Long result, final Throwable t) {
                        downloadLatch2.countDown();
                        System.out.println("downloaded file sized: " + result);
                    }
                });
        downloadLatch2.await();
        streamToDownloadTo.close();

        /*
         * OpenDownloadStream
         */
        final CountDownLatch downloadLatch3 = new CountDownLatch(1);
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
                        downloadLatch3.countDown();
                    }
                });
            }
        });
        downloadLatch3.await();

        /*
         * OpenDownloadStreamByName
         */
        System.out.println("ByName");
        dstByteBuffer.clear();
        final CountDownLatch downloadLatch4 = new CountDownLatch(1);
        final GridFSDownloadStream downloadStreamByName = gridFSBucket.openDownloadStream("sampleData");
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
                        downloadLatch4.countDown();
                    }
                });
            }
        });
        downloadLatch4.await();

        /*
         * Rename
         */
        final CountDownLatch renameLatch = new CountDownLatch(1);
        gridFSBucket.rename(fileId, "mongodbTutorial", new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Renamed file");
                renameLatch.countDown();
            }
        });
        renameLatch.await();

        /*
         * Delete
         */
        final CountDownLatch deleteLatch = new CountDownLatch(1);
        gridFSBucket.delete(fileId, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Deleted file");
                deleteLatch.countDown();
            }
        });
        deleteLatch.await();

        // Final cleanup
        final CountDownLatch dropLatch2 = new CountDownLatch(1);
        database.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch2.countDown();
            }
        });
        dropLatch2.await();
        System.out.println("Finished");
    }

    private GridFSTour() {
    }
}

/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gridfs;

import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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
        database.drop();

        GridFSBucket gridFSBucket = GridFSBuckets.create(database);

        /*
         * UploadFromStream Example
         */
        // Get the input stream
        InputStream streamToUploadFrom = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));

        // Create some custom options
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(1024)
                .metadata(new Document("type", "presentation"));

        ObjectId fileId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options);
        streamToUploadFrom.close();
        System.out.println("The fileId of the uploaded file is: " + fileId.toHexString());

        /*
         * OpenUploadStream Example
         */

        // Get some data to write
        byte[] data = "Data to upload into GridFS".getBytes(StandardCharsets.UTF_8);


        GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("sampleData");
        uploadStream.write(data);
        uploadStream.close();
        System.out.println("The fileId of the uploaded file is: " + uploadStream.getObjectId().toHexString());

        /*
         * Find documents
         */
        gridFSBucket.find().forEach(new Block<GridFSFile>() {
            @Override
            public void apply(final GridFSFile gridFSFile) {
                System.out.println(gridFSFile.getFilename());
            }
        });

        /*
         * Find documents with a filter
         */
        gridFSBucket.find(eq("metadata.contentType", "image/png")).forEach(
                new Block<GridFSFile>() {
                    @Override
                    public void apply(final GridFSFile gridFSFile) {
                        System.out.println(gridFSFile.getFilename());
                    }
                });

        /*
         * DownloadToStream
         */
        FileOutputStream streamToDownloadTo = new FileOutputStream("/tmp/mongodb-tutorial.txt");
        gridFSBucket.downloadToStream(fileId, streamToDownloadTo);
        streamToDownloadTo.close();

        /*
         * DownloadToStreamByName
         */
        streamToDownloadTo = new FileOutputStream("/tmp/mongodb-tutorial.txt");
        GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions().revision(0);
        gridFSBucket.downloadToStream("mongodb-tutorial", streamToDownloadTo, downloadOptions);
        streamToDownloadTo.close();

        /*
         * OpenDownloadStream
         */
        GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(fileId);
        int fileLength = (int) downloadStream.getGridFSFile().getLength();
        byte[] bytesToWriteTo = new byte[fileLength];
        downloadStream.read(bytesToWriteTo);
        downloadStream.close();

        System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));

        /*
         * OpenDownloadStreamByName
         */

        downloadStream = gridFSBucket.openDownloadStream("sampleData");
        fileLength = (int) downloadStream.getGridFSFile().getLength();
        bytesToWriteTo = new byte[fileLength];
        downloadStream.read(bytesToWriteTo);
        downloadStream.close();

        System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));

        /*
         * Rename
         */
        gridFSBucket.rename(fileId, "mongodbTutorial");

        /*
         * Delete
         */
        gridFSBucket.delete(fileId);


        database.drop();
    }

    private GridFSTour() {
    }
}

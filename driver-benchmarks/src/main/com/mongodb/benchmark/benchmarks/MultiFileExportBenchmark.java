/*
 * Copyright 2016-present MongoDB, Inc.
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
 *
 */

package com.mongodb.benchmark.benchmarks;

import com.mongodb.benchmark.framework.BenchmarkResult;
import com.mongodb.benchmark.framework.BenchmarkRunner;
import com.mongodb.benchmark.framework.TextBasedBenchmarkResultWriter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiFileExportBenchmark extends AbstractMongoBenchmark {

    private MongoDatabase database;

    private MongoCollection<RawBsonDocument> collection;

    private ExecutorService fileWritingService;
    private ExecutorService documentReadingService;
    private File tempDirectory;

    @Override
    public String getName() {
        return "LDJSON multi-file export";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        database = client.getDatabase(DATABASE_NAME);
        collection = database.getCollection(COLLECTION_NAME, RawBsonDocument.class);

        database.drop();

        importJsonFiles();

        fileWritingService = Executors.newFixedThreadPool(FILE_WRITING_THREAD_POOL_SIZE);
        documentReadingService = Executors.newFixedThreadPool(MONGODB_READING_THREAD_POOL_SIZE);
    }

    @Override
    public void tearDown() throws Exception {
        fileWritingService.shutdown();
        documentReadingService.shutdown();
        fileWritingService.awaitTermination(1, TimeUnit.MINUTES);
        documentReadingService.awaitTermination(1, TimeUnit.MINUTES);

        super.tearDown();
    }

    @Override
    public void before() throws Exception {
        super.before();

        tempDirectory = Files.createTempDirectory("LDJSON_MULTI").toFile();
    }

    @Override
    public void after() throws Exception {
        for (File file : tempDirectory.listFiles()) {
            file.delete();
        }

        tempDirectory.delete();
        super.after();
    }

    @Override
    public void run() throws Exception {
        CountDownLatch latch = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            documentReadingService.submit(exportJsonFile(i, latch));
        }

        latch.await(1, TimeUnit.MINUTES);
    }

    @Override
    public int getBytesPerRun() {
        return 557610482;
    }

    private Runnable exportJsonFile(final int fileId, final CountDownLatch latch) {
        return new Runnable() {
            @Override
            public void run() {
                List<RawBsonDocument> documents = collection.find(new BsonDocument("fileId", new BsonInt32(fileId)))
                        .batchSize(5000)
                        .into(new ArrayList<RawBsonDocument>(5000));
                fileWritingService.submit(writeJsonFile(fileId, documents, latch));
            }
        };
    }

    private Runnable writeJsonFile(final int fileId, final List<RawBsonDocument> documents, final CountDownLatch latch) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    Writer writer = new OutputStreamWriter(
                            new FileOutputStream(new File(tempDirectory, String.format("%03d", fileId) + ".txt")), StandardCharsets.UTF_8);
                    try {
                        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
                        for (RawBsonDocument cur : documents) {
                            codec.encode(new JsonWriter(writer), cur, EncoderContext.builder().build());
                            writer.write('\n');
                        }
                    } finally {
                        writer.close();
                    }
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private void importJsonFiles() throws InterruptedException {
        ExecutorService importService = Executors.newFixedThreadPool(FILE_READING_THREAD_POOL_SIZE);

        final CountDownLatch latch = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            final int fileId = i;
            importService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        String resourcePath = "parallel/ldjson_multi/ldjson" + String.format("%03d", fileId) + ".txt";
                        BufferedReader reader = new BufferedReader(readFromRelativePath(resourcePath), 1024 * 64);
                        try {
                            String json;
                            List<BsonDocument> documents = new ArrayList<BsonDocument>(1000);
                            while ((json = reader.readLine()) != null) {
                                BsonDocument document = new BsonDocumentCodec().decode(new JsonReader(json),
                                        DecoderContext.builder().build());
                                document.put("fileId", new BsonInt32(fileId));
                                documents.add(document);
                            }
                            database.getCollection(COLLECTION_NAME, BsonDocument.class).insertMany(documents,
                                    new InsertManyOptions().ordered(false));
                            latch.countDown();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            reader.close();
                        }
                    } catch (IOException e) {
                         throw new RuntimeException(e);
                    }
                }
            });
        }
        latch.await(1, TimeUnit.MINUTES);

        collection.createIndex(new BsonDocument("fileId", new BsonInt32(1)));

        importService.shutdown();
        importService.awaitTermination(1, TimeUnit.MINUTES);
    }

    public static void main(String[] args) throws Exception {
        BenchmarkResult benchmarkResult = new BenchmarkRunner(new MultiFileExportBenchmark(), 0, 1).run();
        new TextBasedBenchmarkResultWriter(System.out).write(benchmarkResult);
    }
}

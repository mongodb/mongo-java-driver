/*
 * * Copyright 2016-present MongoDB, Inc.
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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GridFSMultiFileUploadBenchmark extends AbstractMongoBenchmark {

    private MongoDatabase database;
    private GridFSBucket bucket;

    private ExecutorService fileService;

    @Override
    public String getName() {
        return "GridFS multi-file upload";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        database = client.getDatabase(DATABASE_NAME);
        bucket = GridFSBuckets.create(database);

        database.drop();

        fileService = Executors.newFixedThreadPool(FILE_READING_THREAD_POOL_SIZE);
    }

    @Override
    public void tearDown() throws Exception {
        fileService.shutdown();
        fileService.awaitTermination(1, TimeUnit.MINUTES);

        super.tearDown();
    }

    @Override
    public void before() throws Exception {
        super.before();
        database.drop();
        bucket.uploadFromStream("small", new ByteArrayInputStream(new byte[1]));
    }

    @Override
    public void run() throws Exception {
        CountDownLatch latch = new CountDownLatch(50);

        for (int i = 0; i < 50; i++) {
            fileService.submit(importFile(latch, i));
        }

        latch.await(1, TimeUnit.MINUTES);
    }

    private Runnable importFile(final CountDownLatch latch, final int fileId) {
        return () -> {
            try {
                String fileName = "file" + String.format("%02d", fileId) + ".txt";
                String resourcePath = "parallel/gridfs_multi/" + fileName;
                bucket.uploadFromStream(fileName, streamFromRelativePath(resourcePath),
                        new GridFSUploadOptions().chunkSizeBytes(ONE_MB));
                latch.countDown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public int getBytesPerRun() {
        return 262144000;
    }

    public static void main(String[] args) throws Exception {
        BenchmarkResult benchmarkResult = new BenchmarkRunner(new GridFSMultiFileUploadBenchmark(), 4, 10).run();
        new TextBasedBenchmarkResultWriter(System.out).write(benchmarkResult);
    }
}

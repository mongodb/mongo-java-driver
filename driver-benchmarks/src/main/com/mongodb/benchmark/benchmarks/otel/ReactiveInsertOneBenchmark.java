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

package com.mongodb.benchmark.benchmarks.otel;

import com.mongodb.MongoClientSettings;
import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonReader;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * DriverBench "Small doc insertOne" through the Reactive Streams API, one blocking subscription per operation.
 */
public class ReactiveInsertOneBenchmark extends Benchmark {
    private static final String DATABASE_NAME = "perftest";
    private static final String COLLECTION_NAME = "corpus";

    private final String resourcePath;
    private final int numIterations;
    private final MongoClientSettings settings;
    private MongoClient client;
    private MongoCollection<Document> collection;
    private Document document;
    private int fileLength;

    public ReactiveInsertOneBenchmark(final String name, final String resourcePath, final int numIterations,
            final MongoClientSettings settings) {
        super(name + " doc insertOne");
        this.resourcePath = resourcePath;
        this.numIterations = numIterations;
        this.settings = settings;
    }

    @Override
    public void setUp() throws Exception {
        client = MongoClients.create(settings);
        collection = client.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME);
        byte[] bytes = readAllBytesFromRelativePath(resourcePath);
        fileLength = bytes.length;
        document = new DocumentCodec().decode(new JsonReader(new String(bytes, StandardCharsets.UTF_8)),
                DecoderContext.builder().build());
        Mono.from(client.getDatabase(DATABASE_NAME).drop()).block();
    }

    @Override
    public void before() {
        Mono.from(collection.drop()).block();
    }

    @Override
    public void run() {
        for (int i = 0; i < numIterations; i++) {
            document.remove("_id");
            Mono.from(collection.insertOne(document)).block();
        }
    }

    @Override
    public void tearDown() {
        client.close();
    }

    @Override
    public int getBytesPerRun() {
        return fileLength * numIterations;
    }
}

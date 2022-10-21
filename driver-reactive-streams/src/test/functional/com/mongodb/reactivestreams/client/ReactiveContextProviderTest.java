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
 */

package com.mongodb.reactivestreams.client;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT_PROVIDER;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.assertContextPassedThrough;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;


public class ReactiveContextProviderTest {
    private MongoClient mongoClient;

    @BeforeEach
    public void setup() {
        getCollection().insertMany(rangeClosed(1, 11)
                .boxed()
                .map(i -> BsonDocument.parse(format("{a: %s}", i)))
                .collect(Collectors.toList()));
    }

    @AfterEach
    public void tearDown() {
        if (mongoClient != null) {
            getCollection().drop();
            mongoClient.close();
            mongoClient = null;
        }
    }


    @SuppressWarnings("deprecation")
    @TestFactory
    @DisplayName("test context passed through when using first")
    List<DynamicTest> testMongoIterableFirstPassesTheContext() {
        return asList(
                dynamicTest("Aggregate Publisher", () -> {
                    getCollection().aggregate(singletonList(Aggregates.match(Filters.gt("a", 5)))).first();
                    assertContextPassedThrough();
                }),
                dynamicTest("Distinct Publisher", () -> {
                    getCollection().distinct("a", Integer.class).first();
                    assertContextPassedThrough();
                }),
                dynamicTest("Find Publisher", () -> {
                    getCollection().find().first();
                    assertContextPassedThrough();
                }),
                dynamicTest("List Collections Publisher", () -> {
                    getDatabase().listCollections().first();
                    assertContextPassedThrough();
                }),
                dynamicTest("List Databases Publisher", () -> {
                    getMongoClient().listDatabases().first();
                    assertContextPassedThrough();
                }),
                dynamicTest("List Indexes Publisher", () -> {
                    getCollection().listIndexes().first();
                    assertContextPassedThrough();
                }),
                dynamicTest("Map Reduce Publisher", () -> {
                    getCollection().mapReduce(
                            "function () { emit('a', this.a) }",
                            "function (k, v) { return Array.sum(v)}").first();
                    assertContextPassedThrough();
                })
        );
    }

    private MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = new SyncMongoClient(
                    MongoClients.create(getMongoClientSettingsBuilder()
                            .contextProvider(CONTEXT_PROVIDER)
                            .build()));
        }
        return mongoClient;
    }

    private MongoDatabase getDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    private MongoCollection<BsonDocument> getCollection() {
        return getDatabase().getCollection("contextViewRegressionTest", BsonDocument.class);
    }

}

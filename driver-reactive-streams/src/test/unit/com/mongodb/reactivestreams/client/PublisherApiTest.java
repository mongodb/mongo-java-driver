/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;


import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class PublisherApiTest {

    @TestFactory
    @DisplayName("test that publisher apis matches sync")
    List<DynamicTest> testPublisherApiMatchesSyncApi() {
        return asList(
                dynamicTest("Client Session Api", () -> assertApis(com.mongodb.client.ClientSession.class, ClientSession.class)),
                dynamicTest("MongoClient Api", () -> assertApis(com.mongodb.client.MongoClient.class, MongoClient.class)),
                dynamicTest("MongoDatabase Api", () -> assertApis(com.mongodb.client.MongoDatabase.class, MongoDatabase.class)),
                dynamicTest("MongoCollection Api", () -> assertApis(com.mongodb.client.MongoCollection.class, MongoCollection.class)),
                dynamicTest("Aggregate Api", () -> assertApis(AggregateIterable.class, AggregatePublisher.class)),
                dynamicTest("Change Stream Api", () -> assertApis(ChangeStreamIterable.class, ChangeStreamPublisher.class)),
                dynamicTest("Distinct Api", () -> assertApis(DistinctIterable.class, DistinctPublisher.class)),
                dynamicTest("Find Api", () -> assertApis(FindIterable.class, FindPublisher.class)),
                dynamicTest("List Collections Api", () -> assertApis(ListCollectionsIterable.class, ListCollectionsPublisher.class)),
                dynamicTest("List Databases Api", () -> assertApis(ListDatabasesIterable.class, ListDatabasesPublisher.class)),
                dynamicTest("List Indexes Api", () -> assertApis(ListIndexesIterable.class, ListIndexesPublisher.class)),
                dynamicTest("Map Reduce Api", () -> assertApis(MapReduceIterable.class, MapReducePublisher.class)),
                dynamicTest("GridFS Buckets Api", () -> assertApis(com.mongodb.client.gridfs.GridFSBuckets.class, GridFSBuckets.class)),
                dynamicTest("GridFS Find Api", () -> assertApis(GridFSFindIterable.class, GridFSFindPublisher.class))
        );
    }

    void assertApis(final Class<?> syncApi, final Class<?> publisherApi) {
        List<String> syncMethods = getMethodNames(syncApi);
        List<String> publisherMethods = getMethodNames(publisherApi);
        assertIterableEquals(syncMethods, publisherMethods, format("%s != %s%nSync: %s%nPub:  %s",
                syncApi.getSimpleName(), publisherApi.getSimpleName(), syncMethods, publisherMethods));
    }

    private static final List<String> SYNC_ONLY_APIS = asList("iterator", "cursor", "map", "into", "spliterator", "forEach");
    private static final List<String> PUBLISHER_ONLY_APIS =  asList("batchCursor", "getBatchSize", "subscribe");

    private List<String> getMethodNames(final Class<?> clazz) {
        return Arrays.stream(clazz.getMethods())
                .map(Method::getName)
                .distinct()
                .filter(n -> !SYNC_ONLY_APIS.contains(n) && !PUBLISHER_ONLY_APIS.contains(n))
                .sorted()
                .collect(Collectors.toList());
    }

}

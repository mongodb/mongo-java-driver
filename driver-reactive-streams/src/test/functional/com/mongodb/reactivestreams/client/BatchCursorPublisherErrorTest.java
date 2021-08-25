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

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.Fixture.drop;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabase;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient;
import static com.mongodb.reactivestreams.client.Fixture.serverVersionAtLeast;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class BatchCursorPublisherErrorTest {

    private MongoCollection<Document> collection;

    @BeforeEach
    public void setup() {
        assumeTrue(serverVersionAtLeast(3, 6));
        collection = getDefaultDatabase().getCollection("changeStreamsCancellationTest");
        Mono.from(collection.insertMany(rangeClosed(1, 11)
                .boxed()
                .map(i -> Document.parse(format("{a: %s}", i)))
                .collect(Collectors.toList()))
        ).block(TIMEOUT_DURATION);
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            drop(collection.getNamespace());
        }
    }

    @SuppressWarnings("deprecation")
    @TestFactory
    @DisplayName("test batch cursors close the cursor if onNext throws an error")
    List<DynamicTest> testBatchCursorThrowsAnError() {
        return asList(
                dynamicTest("Aggregate Publisher",
                        () -> assertErrorHandling(collection.aggregate(singletonList(Aggregates.match(Filters.gt("a", 5)))))),
                dynamicTest("Distinct Publisher", () -> assertErrorHandling(collection.distinct("a", Integer.class))),
                dynamicTest("Find Publisher", () -> assertErrorHandling(collection.find())),
                dynamicTest("List Collections Publisher", () -> assertErrorHandling(getDefaultDatabase().listCollections())),
                dynamicTest("List Databases Publisher", () -> assertErrorHandling(getMongoClient().listDatabaseNames())),
                dynamicTest("List Indexes Publisher", () -> assertErrorHandling(collection.listIndexes())),
                dynamicTest("Map Reduce Publisher", () -> assertErrorHandling(collection.mapReduce(
                        "function () { emit('a', this.a) }",
                        "function (k, v) { return Array.sum(v)}")))
        );
    }

    <T> void assertErrorHandling(final Publisher<T> publisher) {
        TestSubscriber<T> subscriber = new TestSubscriber<>();
        subscriber.doOnSubscribe(sub -> sub.request(5));
        subscriber.doOnNext(t -> {
            throw new RuntimeException("Some user error");
        });
        publisher.subscribe(subscriber);
        assertDoesNotThrow(Fixture::waitForLastServerSessionPoolRelease);
        subscriber.assertNoTerminalEvent();
    }

}

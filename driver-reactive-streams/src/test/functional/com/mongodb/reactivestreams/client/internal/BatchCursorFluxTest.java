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
package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.TestSubscriber;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.drop;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

@ExtendWith(MockitoExtension.class)
public class BatchCursorFluxTest {

    private MongoClient client;
    private TestCommandListener commandListener;
    private MongoCollection<Document> collection;

    @Mock
    private BatchCursorPublisher<Document> batchCursorPublisher;

    @BeforeEach
    public void setUp() {
        commandListener = new TestCommandListener(singletonList("commandStartedEvent"), asList("insert", "killCursors"));
        MongoClientSettings mongoClientSettings = getMongoClientBuilderFromConnectionString().addCommandListener(commandListener).build();
        client = MongoClients.create(mongoClientSettings);
        collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
        drop(collection.getNamespace());
    }

    @AfterEach
    public void tearDown() {
        try {
            if (collection != null) {
                drop(collection.getNamespace());
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    public void testBatchCursorRespectsTheSetBatchSize() {
        List<Document> docs = createDocs(20);
        Mono.from(collection.insertMany(docs)).block(TIMEOUT_DURATION);

        TestSubscriber<Document> subscriber = new TestSubscriber<>();
        collection.find().batchSize(5).subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(5);
        subscriber.assertReceivedOnNext(docs.subList(0, 5));
        assertCommandNames(singletonList("find"));

        subscriber.requestMore(5);
        subscriber.assertReceivedOnNext(docs.subList(0, 10));
        assertCommandNames(asList("find", "getMore"));

        subscriber.requestMore(10);
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertNoTerminalEvent();
        assertCommandNames(asList("find", "getMore", "getMore", "getMore"));

        subscriber.requestMore(1);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();
        assertCommandNames(asList("find", "getMore", "getMore", "getMore", "getMore"));
    }

    @Test
    public void testBatchCursorSupportsBatchSizeZero() {
        List<Document> docs = createDocs(200);
        Mono.from(collection.insertMany(docs)).block(TIMEOUT_DURATION);

        TestSubscriber<Document> subscriber = new TestSubscriber<>();
        collection.find().batchSize(0).subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(100);
        subscriber.assertReceivedOnNext(docs.subList(0, 100));
        assertCommandNames(singletonList("find"));

        subscriber.requestMore(101);
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();
        assertCommandNames(asList("find", "getMore"));
    }

    @Test
    public void testBatchCursorConsumesBatchesThenGetMores() {
        List<Document> docs = createDocs(99);
        Mono.from(collection.insertMany(docs)).block(TIMEOUT_DURATION);

        TestSubscriber<Document> subscriber = new TestSubscriber<>();
        collection.find().batchSize(50).subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(25);
        subscriber.assertReceivedOnNext(docs.subList(0, 25));
        assertCommandNames(singletonList("find"));

        subscriber.requestMore(25);
        subscriber.assertReceivedOnNext(docs.subList(0, 50));
        assertCommandNames(singletonList("find"));

        subscriber.requestMore(25);
        subscriber.assertReceivedOnNext(docs.subList(0, 75));
        subscriber.assertNoTerminalEvent();
        assertCommandNames(asList("find", "getMore"));

        subscriber.requestMore(25);
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();
        assertCommandNames(asList("find", "getMore"));
    }

    @Test
    public void testBatchCursorDynamicBatchSize() {
        List<Document> docs = createDocs(200);
        Mono.from(collection.insertMany(docs)).block(TIMEOUT_DURATION);

        TestSubscriber<Document> subscriber = new TestSubscriber<>();
        FindPublisher<Document> findPublisher = collection.find();
        findPublisher.subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(20);
        subscriber.assertReceivedOnNext(docs.subList(0, 20));
        assertCommandNames(singletonList("find"));

        subscriber.requestMore(150);
        subscriber.assertReceivedOnNext(docs.subList(0, 170));
        assertCommandNames(asList("find", "getMore"));

        subscriber.requestMore(40);
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();
        assertCommandNames(asList("find", "getMore", "getMore"));
    }

    @Test
    public void testBatchCursorCompletesAsExpectedWithLimit() {
        List<Document> docs = createDocs(100);
        Mono.from(collection.insertMany(docs)).block(TIMEOUT_DURATION);

        TestSubscriber<Document> subscriber = new TestSubscriber<>();
        FindPublisher<Document> findPublisher = collection.find().limit(100);
        findPublisher.subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(100);
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();
        assertCommandNames(singletonList("find"));
    }

    @Test
    public void testBatchCursorDynamicBatchSizeOnReuse() {
        List<Document> docs = createDocs(200);
        Mono.from(collection.insertMany(docs)).block(TIMEOUT_DURATION);

        TestSubscriber<Document> subscriber = new TestSubscriber<>();
        FindPublisher<Document> findPublisher = collection.find();
        findPublisher.subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(100);
        subscriber.assertReceivedOnNext(docs.subList(0, 100));
        assertCommandNames(singletonList("find"));

        subscriber.requestMore(200);
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();
        assertCommandNames(asList("find", "getMore"));

        commandListener.reset();
        subscriber = new TestSubscriber<>();
        findPublisher.subscribe(subscriber);
        assertCommandNames(emptyList());

        subscriber.requestMore(Long.MAX_VALUE);
        subscriber.assertNoErrors();
        subscriber.assertReceivedOnNext(docs);
        subscriber.assertTerminalEvent();
        assertCommandNames(singletonList("find"));
    }

    @Test
    public void testCalculateDemand() {
        BatchCursorFlux<Document> batchCursorFlux = new BatchCursorFlux<>(batchCursorPublisher);

        assertAll("Calculating demand",
                () -> assertEquals(0, batchCursorFlux.calculateDemand(0)),
                () -> assertEquals(10, batchCursorFlux.calculateDemand(10)),
                () -> assertEquals(0, batchCursorFlux.calculateDemand(-10)),
                () -> assertEquals(Integer.MAX_VALUE, batchCursorFlux.calculateDemand(Integer.MAX_VALUE)),
                () -> assertEquals(Long.MAX_VALUE, batchCursorFlux.calculateDemand(Long.MAX_VALUE)),
                () -> assertEquals(Long.MAX_VALUE, batchCursorFlux.calculateDemand(1)),
                () -> assertEquals(0, batchCursorFlux.calculateDemand(-Long.MAX_VALUE))
        );
    }

    @Test
    public void testCalculateBatchSize() {
        BatchCursorFlux<Document> batchCursorFlux = new BatchCursorFlux<>(batchCursorPublisher);

        when(batchCursorPublisher.getBatchSize()).thenReturn(null);
        assertAll("Calculating batch size with dynamic batch size",
                () -> assertEquals(2, batchCursorFlux.calculateBatchSize(1)),
                () -> assertEquals(1000, batchCursorFlux.calculateBatchSize(1000)),
                () -> assertEquals(Integer.MAX_VALUE, batchCursorFlux.calculateBatchSize(Integer.MAX_VALUE)),
                () -> assertEquals(Integer.MAX_VALUE, batchCursorFlux.calculateBatchSize(Long.MAX_VALUE))
        );


        when(batchCursorPublisher.getBatchSize()).thenReturn(10);
        assertAll("Calculating batch size with set batch size",
                () -> assertEquals(10, batchCursorFlux.calculateBatchSize(100)),
                () -> assertEquals(10, batchCursorFlux.calculateBatchSize(Long.MAX_VALUE)),
                () -> assertEquals(10, batchCursorFlux.calculateBatchSize(1))
        );

    }


    private void assertCommandNames(final List<String> commandNames) {
        assertIterableEquals(commandNames,
                commandListener.getCommandStartedEvents().stream().map(CommandEvent::getCommandName).collect(Collectors.toList()));
    }

    private List<Document> createDocs(final int amount) {
        return IntStream.rangeClosed(1, amount)
                .boxed()
                .map(i -> new Document("_id", i))
                .collect(Collectors.toList());
    }

}

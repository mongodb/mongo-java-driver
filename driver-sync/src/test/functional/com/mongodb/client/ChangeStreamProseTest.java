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

package com.mongodb.client;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.internal.MongoChangeStreamCursorImpl;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.SplitEvent;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.mongodb.internal.operation.ChangeStreamOperationProseTestSpecification;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.CrudTestHelper.repeat;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.client.model.Updates.set;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See <a href="https://github.com/mongodb/specifications/tree/master/source/change-streams/tests/README.md#prose-tests">Prose Tests</a>.
 * <p>
 * See also {@link ChangeStreamOperationProseTestSpecification}.
 */
public class ChangeStreamProseTest {
    private static final MongoNamespace NAMESPACE = new MongoNamespace(ChangeStreamProseTest.class.getSimpleName(), "test");

    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    @BeforeEach
    void beforeEach() {
        assumeTrue(canRunTests());
        client = createClient(getMongoClientSettings());
        database = client.getDatabase(NAMESPACE.getDatabaseName());
        database.drop();
        collection = database.getCollection(NAMESPACE.getCollectionName());
    }

    @AfterEach
    void afterEach() {
        if (client != null) {
            client.close();
        }
    }

    protected MongoClient createClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    static class ChangeStreamWatcher implements Runnable {
        private volatile boolean interruptedExceptionOccurred = false;
        private final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;

        ChangeStreamWatcher(final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
            this.cursor = cursor;
        }

        @Override
        public void run() {
            try {
                cursor.next();
            } catch (MongoInterruptedException e) {
                interruptedExceptionOccurred = true;
            } finally {
                cursor.close();
            }
        }

        boolean getInterruptedExceptionOccurred() {
            return interruptedExceptionOccurred;
        }
    }

    //
    // Test that MongoInterruptedException is not retryable so that a thread can be interrupted.
    //
    @Test
    void testThreadInterrupted() throws InterruptedException {
        ChangeStreamWatcher watcher = new ChangeStreamWatcher(collection.watch().cursor());
        Thread t = new Thread(watcher);
        t.start();
        t.interrupt();
        t.join();
        assertTrue(watcher.getInterruptedExceptionOccurred());
    }

    //
    // Test that the ChangeStream continuously tracks the last seen resumeToken.
    //
    @Test
    void test01ChangeStreamTracksResumeToken() {

        try (MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator()) {
            collection.insertOne(Document.parse("{x: 1}"));
            BsonDocument initialResumeToken = cursor.next().getResumeToken();
            assertNotNull(initialResumeToken);

            collection.insertOne(Document.parse("{x: 2}"));
            BsonDocument nextResumeToken = cursor.next().getResumeToken();
            assertNotNull(nextResumeToken);
            assertNotEquals(initialResumeToken, nextResumeToken);
        }
    }

    //
    // Test that the ChangeStream will throw an exception if the server response is missing the resume token (if wire version is < 8).
    //
    @Test
    void test02MissingResumeTokenThrowsException() {
        try (MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch(singletonList(Aggregates.project(Document.parse("{ _id : 0 }"))))
                .iterator()) {
            collection.insertOne(Document.parse("{ x: 1 }"));
            cursor.next();
            fail();
        } catch (MongoChangeStreamException | MongoQueryException ignored) {
        }
    }

    //
    // Test that the ChangeStream will automatically resume one time on a resumable error (including not primary)
    // with the initial pipeline and options, except for the addition/update of a resumeToken.
    //
    @Test
    void test03ResumeOneTimeOnError() throws Exception {
        BsonDocument configureFailPoint = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument("failCommands", new BsonArray(singletonList(new BsonString("getMore"))))
                        .append("errorCode", new BsonInt32(10107))
                        .append("errorLabels", new BsonArray(singletonList(new BsonString("ResumableChangeStreamError")))));
        try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary());
             MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            collection.insertOne(Document.parse("{ x: 1 }"));
            assertNotNull(cursor.next());
        }
    }

    //
    // Test that ChangeStream will not attempt to resume on any error encountered while executing an aggregate command.
    //
    @Test
    protected void test04NoResumeForAggregateErrors() {
        boolean exceptionFound = false;
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = null;

        //noinspection TryFinallyCanBeTryWithResources
        try {
            cursor = collection.watch(singletonList(Document.parse("{ $unsupportedStage: { _id : 0 } }"))).cursor();
        } catch (MongoCommandException e) {
            exceptionFound = true;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        assertTrue(exceptionFound);
    }

    //
    // Ensure that a cursor returned from an aggregate command with a cursor id and an initial empty batch
    // is not closed on the driver side.
    //
    @Test
    protected void test07CursorNotClosed() {
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        assertNotNull(cursor.getServerCursor());
        cursor.close();
    }

    //
    // 11. For a ChangeStream under these conditions:
    //   Running against a server >=4.0.7.
    //   The batch is empty or has been iterated to the last document.
    // Expected result:
    //   getResumeToken must return the postBatchResumeToken from the current command response.
    //
    @Test
    protected void test11GetResumeTokenReturnsPostBatchResumeToken() throws NoSuchFieldException, IllegalAccessException {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor();
        assertNull(cursor.getResumeToken());
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        try {
            cursor.next();

            // use reflection to access the postBatchResumeToken
            AggregateResponseBatchCursor<?> batchCursor = getBatchCursor(cursor);
            assertEquals(cursor.getResumeToken(), batchCursor.getPostBatchResumeToken());
        } finally {
            cursor.close();
        }
    }

    //
    // 13. For a ChangeStream under these conditions:
    //   The batch is not empty.
    //   The batch has been iterated up to but not including the last element.
    // Expected result:
    //   getResumeToken must return the _id of the previous document returned.
    //
    @Test
    protected void test13GetResumeTokenEqualsIdOfPreviousDocument() {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().batchSize(3).cursor()) {
            collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
            collection.insertOne(Document.parse("{ _id: 43, x: 1 }"));
            collection.insertOne(Document.parse("{ _id: 44, x: 1 }"));
            cursor.next();
            assertEquals(cursor.next().getResumeToken(), cursor.getResumeToken());
        }
    }

    //
    // 14. For a ChangeStream under these conditions: (startAfter only supported for 4.2)
    //   The batch is not empty.
    //   The batch hasn’t been iterated at all.
    //   Only the initial aggregate command has been executed.
    // Expected result:
    //   getResumeToken must return startAfter from the initial aggregate if the option was specified.
    //   If startAfter is not specified, the getResumeToken result must be empty.
    //
    @Test
    protected void test14GetResumeTokenReturnsStartAfter() {
        BsonDocument resumeToken;
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor();
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        collection.insertOne(Document.parse("{ _id: 43, x: 1 }"));
        try {
            resumeToken = cursor.next().getResumeToken();
        } finally {
            cursor.close();
        }

        cursor = collection.watch().startAfter(resumeToken).cursor();
        try {
            assertEquals(resumeToken, cursor.getResumeToken());
        } finally {
            cursor.close();
        }

        cursor = collection.watch().cursor();
        try {
            assertNull(cursor.getResumeToken());
        } finally {
            cursor.close();
        }
    }

    //
    // 14. For a ChangeStream under these conditions:
    //   The batch is not empty.
    //   The batch hasn’t been iterated at all.
    //   Only the initial aggregate command has been executed.
    // Expected result:
    //   getResumeToken must return resumeAfter from the initial aggregate if the option was specified.
    //   If resumeAfter is not specified, the getResumeToken result must be empty.
    //
    @Test
    protected void test14GetResumeTokenReturnsResumeAfter() {
        BsonDocument resumeAfterResumeToken;
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor();
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        collection.insertOne(Document.parse("{ _id: 43, x: 1 }"));
        try {
            resumeAfterResumeToken = cursor.next().getResumeToken();
        } finally {
            cursor.close();
        }

        cursor = collection.watch().resumeAfter(resumeAfterResumeToken).cursor();
        try {
            assertEquals(resumeAfterResumeToken, cursor.getResumeToken());
        } finally {
            cursor.close();
        }

        cursor = collection.watch().cursor();
        try {
            assertNull(cursor.getResumeToken());
        } finally {
            cursor.close();
        }
    }

    //
    // 15. For a ChangeStream under these conditions:
    //   Running against a server >=4.0.7.
    //   The batch is not empty.
    //   The batch hasn’t been iterated at all.
    //   The stream has iterated beyond a previous batch and a getMore command has just been executed.
    // Expected result:
    //   getResumeToken must return the postBatchResumeToken from the previous command response.
    //
    @Test
    protected void test15GetResumeTokenReturnsPostBatchResumeTokenAfterGetMore()
            throws NoSuchFieldException, IllegalAccessException {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
            // use reflection to access the postBatchResumeToken
            AggregateResponseBatchCursor<?> batchCursor = getBatchCursor(cursor);

            assertNotNull(batchCursor.getPostBatchResumeToken());

            // resume token should be null before iteration
            assertNull(cursor.getResumeToken());

            cursor.next();
            assertEquals(cursor.getResumeToken(), batchCursor.getPostBatchResumeToken());
        }
    }

    @Test
    protected void test19SplitChangeStreamEvents() {
        assumeTrue(serverVersionAtLeast(6, 0));
        collection.drop();
        database.createCollection(
                collection.getNamespace().getCollectionName(),
                new CreateCollectionOptions().changeStreamPreAndPostImagesOptions(
                        new ChangeStreamPreAndPostImagesOptions(true)));

        // #. Insert into _C_ a document at least 10mb in size ...
        int mb10 = 10 * 1024 * 1024;
        collection.insertOne(new Document("value", new BsonString(repeat(mb10, "q"))));

        // #. Create a change stream _S_ by calling ``watch`` on _C_ with
        //    pipeline ``[{ "$changeStreamSplitLargeEvent": {} }]`` and ``fullDocumentBeforeChange=required``.
        ChangeStreamIterable<Document> changeStream = collection
                .watch(singletonList(Document.parse("{ $changeStreamSplitLargeEvent: {} }")))
                .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED);

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
            // #. Call ``updateOne`` on _C_ with an empty ``query`` and an update setting
            //    the field to a new large value
            collection.updateOne(new BsonDocument(), set("value", new BsonString(repeat(mb10, "z"))));

            // #. Collect two events from _S_.
            ChangeStreamDocument<Document> e1 = Assertions.assertNotNull(cursor.tryNext());
            ChangeStreamDocument<Document> e2 = Assertions.assertNotNull(cursor.tryNext());

            // #. Assert that the events collected have ``splitEvent`` fields ..., in that order.
            assertEquals(new SplitEvent(1, 2), e1.getSplitEvent());
            assertEquals(new SplitEvent(2, 2), e2.getSplitEvent());
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    protected void testNameSpaceTypeAbsentChangeStreamEvents() {
        assumeTrue(serverVersionAtLeast(8, 1));
        collection.drop();

        ChangeStreamIterable<Document> changeStream = database
                .watch()
                .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED);

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {

            collection.insertOne(new Document("test", new BsonString("test")));

            ChangeStreamDocument<Document> e1 = Assertions.assertNotNull(cursor.tryNext());

            assertNull(e1.getNamespaceType());
            assertNull(e1.getNamespaceTypeString());
        }
    }

    /**
     * Not a prose spec test.
     */
    @Test
    void shouldDoOneServerSelectionForResumeAttempt() throws Exception {
        //given
        AtomicInteger serverSelectionCounter = new AtomicInteger();
        BsonTimestamp startTime = new BsonTimestamp(toIntExact(Instant.now().getEpochSecond()), 0);
        try (MongoClient mongoClient = createClient(getMongoClientSettingsBuilder()
                .applyToClusterSettings(builder -> builder.serverSelector(clusterDescription -> {
                    serverSelectionCounter.incrementAndGet();
                    return clusterDescription.getServerDescriptions();
                })).build())) {

            MongoCollection<Document> collection = mongoClient
                    .getDatabase(NAMESPACE.getDatabaseName())
                    .getCollection(NAMESPACE.getCollectionName());

            BsonDocument configureFailPoint = BsonDocument.parse("{"
                    + "    configureFailPoint: 'failCommand',"
                    + "    mode: {"
                    + "        times: 1"
                    + "    },"
                    + "    data: {"
                    + "        failCommands: ['getMore'],"
                    + "        errorCode: 9001,"
                    + "        errorLabels: ['ResumableChangeStreamError']"
                    + "    }"
                    + "}");
            // We insert document here, because async cursor performs aggregate and getMore right after we call cursor()
            collection.insertOne(Document.parse("{ x: 1 }"));
            serverSelectionCounter.set(0);

            try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary());
                 MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch()
                         .batchSize(0)
                         .startAtOperationTime(startTime)
                         .cursor()) {

                //when
                ChangeStreamDocument<Document> changeStreamDocument = cursor.next();
                //then
                assertNotNull(changeStreamDocument);
                int actualCountOfServerSelections = serverSelectionCounter.get();
                assertEquals(2, actualCountOfServerSelections,
                        format("Expected 2 server selections (initial aggregate command  + resume attempt aggregate command), but there were %s",
                                actualCountOfServerSelections));
            }
        }
    }

    private boolean canRunTests() {
        return isDiscoverableReplicaSet();
    }

    private AggregateResponseBatchCursor<?> getBatchCursor(final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor)
            throws NoSuchFieldException, IllegalAccessException {
        Field batchCursorField = MongoChangeStreamCursorImpl.class.getDeclaredField("batchCursor");
        batchCursorField.setAccessible(true);
        return (AggregateResponseBatchCursor<?>) (batchCursorField.get(cursor));
    }
}

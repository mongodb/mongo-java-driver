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

package com.mongodb.internal.operation;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoQueryException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.OperationTest;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getReferenceCountAfterTimeout;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.TestOperationHelper.makeAdditionalGetMoreCall;
import static java.util.Collections.singletonList;
import static java.util.stream.Stream.generate;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class CommandBatchCursorFunctionalTest extends OperationTest {

    private ConnectionSource connectionSource;
    private Connection connection;
    private CommandBatchCursor<Document> cursor;

    @BeforeEach
    void setup() {
        List<BsonDocument> documents = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> new BsonDocument("i", new BsonInt32(i)))
                .collect(Collectors.toList());
        getCollectionHelper().insertDocuments(documents);

        connectionSource = getBinding().getWriteConnectionSource();
        connection = connectionSource.getConnection();
    }

    @AfterEach
    void cleanup() {
        ifNotNull(cursor, CommandBatchCursor::close);
        ifNotNull(connectionSource, cs -> {
            getReferenceCountAfterTimeout(cs, 1);
            cs.release();
        });
        ifNotNull(connection, c -> {
            getReferenceCountAfterTimeout(c, 1);
            c.release();
        });
    }

    @Test
    @DisplayName("should exhaust cursor with multiple batches")
    void shouldExhaustCursorAsyncWithMultipleBatches() {
        // given
        BsonDocument commandResult = executeFindCommand(0, 3); // Fetch in batches of size 3
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 3, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        // when
        List<List<Document>> result = cursor.exhaustCursor();

        // then
        assertEquals(4, result.size(), "Expected 4 batches for 10 documents with batch size of 3.");

        int totalDocuments = result.stream().mapToInt(List::size).sum();
        assertEquals(10, totalDocuments, "Expected a total of 10 documents.");
        assertTrue(cursor.isClosed(), "Expected cursor to be closed.");
    }

    @Test
    @DisplayName("should exhaust cursor with closed cursor")
    void shouldExhaustCursorAsyncWithClosedCursor() {
        // given
        BsonDocument commandResult = executeFindCommand(0, 3);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 3, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);
        cursor.close();

        // when & then
        IllegalStateException illegalStateException = assertThrows(IllegalStateException.class, cursor::exhaustCursor);
        assertEquals("Cursor has been closed", illegalStateException.getMessage());
    }

    @Test
    @DisplayName("should exhaust cursor async with empty cursor")
    void shouldExhaustCursorAsyncWithEmptyCursor() {
        // given
        getCollectionHelper().deleteMany(Filters.empty());

        BsonDocument commandResult = executeFindCommand(0, 3); // No documents to fetch
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 3, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        // when
        List<List<Document>> result = cursor.exhaustCursor();

        // then
        assertTrue(result.isEmpty(), "Expected no batches for an empty cursor.");
        assertTrue(cursor.isClosed(), "Expected cursor to be closed.");
    }

    @Test
    @DisplayName("server cursor should not be null")
    void theServerCursorShouldNotBeNull() {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertNotNull(cursor.getServerCursor());
    }

    @Test
    @DisplayName("test server address should not be null")
    void theServerAddressShouldNotNull() {
        BsonDocument commandResult = executeFindCommand();
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertNotNull(cursor.getServerAddress());
    }

    @Test
    @DisplayName("should get Exceptions for operations on the cursor after closing")
    void shouldGetExceptionsForOperationsOnTheCursorAfterClosing() {
        BsonDocument commandResult = executeFindCommand();
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        cursor.close();

        assertDoesNotThrow(() -> cursor.close());
        assertThrows(IllegalStateException.class, () -> cursor.hasNext());
        assertThrows(IllegalStateException.class, () -> cursor.next());
        assertThrows(IllegalStateException.class, () -> cursor.getServerCursor());
    }

    @Test
    @DisplayName("should throw an Exception when going off the end")
    void shouldThrowAnExceptionWhenGoingOffTheEnd() {
        BsonDocument commandResult = executeFindCommand(1);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        cursor.next();
        cursor.next();
        assertThrows(NoSuchElementException.class, () -> cursor.next());
    }

    @Test
    @DisplayName("test cursor remove")
    void testCursorRemove() {
        BsonDocument commandResult = executeFindCommand();
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertThrows(UnsupportedOperationException.class, () -> cursor.remove());
    }

    @Test
    @DisplayName("test normal exhaustion")
    void testNormalExhaustion() {
        BsonDocument commandResult = executeFindCommand();
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertEquals(10, cursorFlatten().size());
    }

    @ParameterizedTest(name = "{index} => limit={0}, batchSize={1}, expectedTotal={2}")
    @MethodSource
    @DisplayName("test limit exhaustion")
    void testLimitExhaustion(final int limit, final int batchSize, final int expectedTotal) {
        BsonDocument commandResult = executeFindCommand(limit, batchSize);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, batchSize, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertEquals(expectedTotal, cursorFlatten().size());

        checkReferenceCountReachesTarget(connectionSource, 1);
        checkReferenceCountReachesTarget(connection, 1);
    }

    @ParameterizedTest(name = "{index} => awaitData={0}, maxTimeMS={1}")
    @MethodSource
    @DisplayName("should block waiting for next batch on a tailable cursor")
    void shouldBlockWaitingForNextBatchOnATailableCursor(final boolean awaitData, final int maxTimeMS) {

        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 1).append("ts", new BsonTimestamp(5, 0)));

        BsonDocument commandResult = executeFindCommand(new BsonDocument("ts",
                new BsonDocument("$gte", new BsonTimestamp(5, 0))), 0, 2, true, awaitData);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, maxTimeMS, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertTrue(cursor.hasNext());
        assertEquals(1, cursor.next().get(0).get("_id"));

        new Thread(() -> {
            sleep(100);
            getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 2).append("ts", new BsonTimestamp(6, 0)));
        }).start();

        assertTrue(cursor.hasNext());
        assertEquals(2, cursor.next().get(0).get("_id"));
    }

    @Test
    @DisplayName("test tryNext with tailable")
    void testTryNextWithTailable() {
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 1).append("ts", new BsonTimestamp(5, 0)));

        BsonDocument commandResult = executeFindCommand(new BsonDocument("ts",
                new BsonDocument("$gte", new BsonTimestamp(5, 0))), 0, 2, true, true);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        List<Document> nextBatch = cursor.tryNext();
        assertNotNull(nextBatch);
        assertEquals(1, nextBatch.get(0).get("_id"));

        nextBatch = cursor.tryNext();
        assertNull(nextBatch);

        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 2).append("ts", new BsonTimestamp(6, 0)));

        nextBatch = cursor.tryNext();
        assertNotNull(nextBatch);
        assertEquals(2, nextBatch.get(0).get("_id"));
    }

    @Test
    @DisplayName("hasNext should throw when cursor is closed in another thread")
    void hasNextShouldThrowWhenCursorIsClosedInAnotherThread() throws InterruptedException {

        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 1).append("ts", new BsonTimestamp(5, 0)));

        BsonDocument commandResult = executeFindCommand(new BsonDocument("ts",
                new BsonDocument("$gte", new BsonTimestamp(5, 0))), 0, 2, true, true);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertTrue(cursor.hasNext());
        assertEquals(1, cursor.next().get(0).get("_id"));

        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            sleep(100);
            cursor.close();
            latch.countDown();
        }).start();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, () -> cursor.hasNext());
    }

    @Test
    @DisplayName("test maxTimeMS")
    void testMaxTimeMS() {
        assumeFalse(isSharded());
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 1).append("ts", new BsonTimestamp(5, 0)));

        long maxTimeMS = 500;
        BsonDocument commandResult = executeFindCommand(new BsonDocument("ts",
                new BsonDocument("$gte", new BsonTimestamp(5, 0))), 0, 2, true, true);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, maxTimeMS, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        List<Document> nextBatch = cursor.tryNext();
        assertNotNull(nextBatch);

        long startTime = System.currentTimeMillis();
        nextBatch = cursor.tryNext();
        long endTime = System.currentTimeMillis();

        assertNull(nextBatch);

        // RACY TEST: no guarantee assertion will fire within the given timeframe
        assertTrue(endTime - startTime < (maxTimeMS + 200));
    }

    @Test
    @DisplayName("test tailable interrupt")
    void testTailableInterrupt() throws InterruptedException {
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 1).append("ts", new BsonTimestamp(5, 0)));

        BsonDocument commandResult = executeFindCommand(new BsonDocument("ts",
                new BsonDocument("$gte", new BsonTimestamp(5, 0))), 0, 2, true, true);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger seen = new AtomicInteger();
        Thread thread = new Thread(() -> {
            try {
                cursor.next();
                seen.incrementAndGet();
                cursor.next();
                seen.incrementAndGet();
            } catch (Exception e) {
                // pass
            } finally {
                latch.countDown();
            }
        });

        thread.start();
        sleep(1000);
        thread.interrupt();
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 2));
        latch.await();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, seen.intValue());
    }

    @Test
    @DisplayName("should kill cursor if limit is reached on initial query")
    void shouldKillCursorIfLimitIsReachedOnInitialQuery() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand(5, 10);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertNotNull(cursor.next());
        assertFalse(cursor.hasNext());
        assertNull(cursor.getServerCursor());
    }

    @Test
    @DisplayName("should kill cursor if limit is reached on getMore")
    void shouldKillCursorIfLimitIsReachedOnGetMore() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand(5, 3);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 3, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        ServerCursor serverCursor = cursor.getServerCursor();
        assertNotNull(serverCursor);
        assertNotNull(cursor.next());
        assertNotNull(cursor.next());

        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connection, 1));
        assertThrows(MongoQueryException.class, () ->
                makeAdditionalGetMoreCall(getNamespace(), serverCursor, connection)
        );
    }

    @Test
    @DisplayName("should release connection source if limit is reached on initial query")
    void shouldReleaseConnectionSourceIfLimitIsReachedOnInitialQuery() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand(5, 10);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertNull(cursor.getServerCursor());
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connectionSource, 1));
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connection, 1));
    }

    @Test
    @DisplayName("should release connection source if limit is reached on getMore")
    void shouldReleaseConnectionSourceIfLimitIsReachedOnGetMore() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand(5, 3);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 3, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertNotNull(cursor.next());
        assertNotNull(cursor.next());
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connectionSource, 1));
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connection, 1));
    }

    @Test
    @DisplayName("test limit with get more")
    void testLimitWithGetMore() {
        BsonDocument commandResult = executeFindCommand(5, 2);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertNotNull(cursor.next());
        assertNotNull(cursor.next());
        assertNotNull(cursor.next());
        assertFalse(cursor.hasNext());
    }

    @Test
    @DisplayName("test limit with large documents")
    void testLimitWithLargeDocuments() {
        String bigString = generate(() -> "x")
                .limit(16000)
                .collect(Collectors.joining());

        IntStream.range(11, 1000).forEach(i ->
                getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", i).append("s", bigString))
        );

        BsonDocument commandResult = executeFindCommand(300, 0);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 0, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertEquals(300, cursorFlatten().size());
    }

    @Test
    @DisplayName("should respect batch size")
    void shouldRespectBatchSize() {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertEquals(2, cursor.getBatchSize());
        assertEquals(2, cursor.next().size());
        assertEquals(2, cursor.next().size());

        cursor.setBatchSize(3);
        assertEquals(3, cursor.getBatchSize());
        assertEquals(3, cursor.next().size());
        assertEquals(3, cursor.next().size());
    }

    @Test
    @DisplayName("should throw cursor not found exception")
    void shouldThrowCursorNotFoundException() {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        ServerCursor serverCursor = cursor.getServerCursor();
        assertNotNull(serverCursor);
        Connection localConnection = connectionSource.getConnection();
        localConnection.command(getNamespace().getDatabaseName(),
                new BsonDocument("killCursors", new BsonString(getNamespace().getCollectionName()))
                        .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId())))),
                NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), new BsonDocumentCodec(), connectionSource.getOperationContext());
        localConnection.release();

        cursor.next();

        MongoCursorNotFoundException exception = assertThrows(MongoCursorNotFoundException.class, () -> cursor.next());
        assertEquals(serverCursor.getId(), exception.getCursorId());
        assertEquals(serverCursor.getAddress(), exception.getServerAddress());
    }

    @Test
    @DisplayName("should report available documents")
    void shouldReportAvailableDocuments() {
        BsonDocument commandResult = executeFindCommand(3);
        cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, commandResult, 2, 0, DOCUMENT_DECODER,
                                          null, connectionSource, connection);

        assertEquals(3, cursor.available());

        cursor.next();
        assertEquals(0, cursor.available());

        assertTrue(cursor.hasNext());
        assertEquals(2, cursor.available());

        cursor.next();
        assertEquals(0, cursor.available());

        assertTrue(cursor.hasNext());
        assertEquals(2, cursor.available());

        cursor.close();
        assertEquals(0, cursor.available());
    }


    private static Stream<Arguments> shouldBlockWaitingForNextBatchOnATailableCursor() {
        return Stream.of(
                arguments(true, 0),
                arguments(true, 100),
                arguments(false, 0));
    }

    private static Stream<Arguments> testLimitExhaustion() {
        return Stream.of(
                arguments(5, 2, 5),
                arguments(5, -2, 2),
                arguments(-5, -2, 5),
                arguments(-5, 2, 5),
                arguments(2, 5, 2),
                arguments(2, -5, 2),
                arguments(-2, 5, 2),
                arguments(-2, -5, 2)
        );
    }

    private BsonDocument executeFindCommand() {
        return executeFindCommand(0);
    }

    private BsonDocument executeFindCommand(final int batchSize) {
        return executeFindCommand(new BsonDocument(), 0, batchSize, false, false);
    }

    private BsonDocument executeFindCommand(final int limit, final int batchSize) {
        return executeFindCommand(new BsonDocument(), limit, batchSize, false, false);
    }

    private BsonDocument executeFindCommand(final BsonDocument filter, final int limit, final int batchSize, final boolean tailable,
            final boolean awaitData) {
        return executeFindCommand(filter, limit, batchSize, tailable, awaitData, ReadPreference.primary());
    }

    private BsonDocument executeFindCommand(final BsonDocument filter, final int limit, final int batchSize,
            final boolean tailable, final boolean awaitData, final ReadPreference readPreference) {
        BsonDocument findCommand = new BsonDocument("find", new BsonString(getCollectionName()))
                .append("filter", filter)
                .append("tailable", BsonBoolean.valueOf(tailable))
                .append("awaitData", BsonBoolean.valueOf(awaitData));

        findCommand.append("limit", new BsonInt32(Math.abs(limit)));
        if (limit >= 0) {
            if (batchSize < 0 && Math.abs(batchSize) < limit) {
                findCommand.append("limit", new BsonInt32(Math.abs(batchSize)));
            } else {
                findCommand.append("batchSize", new BsonInt32(Math.abs(batchSize)));
            }
        }

        BsonDocument results = connection.command(getDatabaseName(), findCommand,
                NoOpFieldNameValidator.INSTANCE, readPreference,
                CommandResultDocumentCodec.create(DOCUMENT_DECODER, FIRST_BATCH),
                connectionSource.getOperationContext());

        assertNotNull(results);
        return results;
    }

    private List<Document> cursorFlatten() {
        List<Document> results = new ArrayList<>();
        while (cursor.hasNext()) {
            results.addAll(cursor.next());
        }
        return results;
    }

}

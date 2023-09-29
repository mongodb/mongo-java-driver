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
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.OperationTest;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget;
import static com.mongodb.ClusterFixture.getAsyncBinding;
import static com.mongodb.ClusterFixture.getConnection;
import static com.mongodb.ClusterFixture.getReferenceCountAfterTimeout;
import static com.mongodb.ClusterFixture.getWriteConnectionSource;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.internal.operation.QueryOperationHelper.makeAdditionalGetMoreCall;
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

public class AsyncCommandBatchCursorFunctionalTest extends OperationTest {

    private AsyncConnectionSource connectionSource;
    private AsyncConnection connection;
    private AsyncCommandBatchCursor<Document> cursor;

    @BeforeEach
    void setup() throws Throwable {
        List<BsonDocument> documents = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> new BsonDocument("i", new BsonInt32(i)))
                .collect(Collectors.toList());
        getCollectionHelper().insertDocuments(documents);

        connectionSource = getWriteConnectionSource(getAsyncBinding());
        connection = getConnection(connectionSource);
    }

    @AfterEach
    void cleanup() {
        ifNotNull(cursor, AsyncCommandBatchCursor::close);
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
    @DisplayName("server cursor should not be null")
    void theServerCursorShouldNotBeNull() {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertNotNull(cursor.getServerCursor());
    }


    @Test
    @DisplayName("should get Exceptions for operations on the cursor after closing")
    void shouldGetExceptionsForOperationsOnTheCursorAfterClosing() {
        BsonDocument commandResult = executeFindCommand();
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        cursor.close();

        assertDoesNotThrow(() -> cursor.close());
        assertThrows(MongoException.class, this::cursorNext);
        assertNull(cursor.getServerCursor());
    }

    @Test
    @DisplayName("should throw an Exception when going off the end")
    void shouldThrowAnExceptionWhenGoingOffTheEnd() {
        BsonDocument commandResult = executeFindCommand(1);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 2, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        cursorNext();
        cursorNext();

        assertThrows(MongoException.class, this::cursorNext);
    }


    @Test
    @DisplayName("test normal exhaustion")
    void testNormalExhaustion() {
        BsonDocument commandResult = executeFindCommand();
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertEquals(10, cursorFlatten().size());
    }

    @ParameterizedTest(name = "{index} => limit={0}, batchSize={1}, expectedTotal={2}")
    @MethodSource
    @DisplayName("test limit exhaustion")
    void testLimitExhaustion(final int limit, final int batchSize, final int expectedTotal) {
        BsonDocument commandResult = executeFindCommand(limit, batchSize);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, limit, batchSize, 0, DOCUMENT_DECODER,
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
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 2, maxTimeMS, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertFalse(cursor.isClosed());
        assertEquals(1, cursorNext().get(0).get("_id"));

        new Thread(() -> {
            sleep(100);
            getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 2).append("ts", new BsonTimestamp(6, 0)));
        }).start();

        assertFalse(cursor.isClosed());
        assertEquals(2, cursorNext().get(0).get("_id"));
    }

    @Test
    @DisplayName("test tailable interrupt")
    void testTailableInterrupt() throws InterruptedException {
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        getCollectionHelper().insertDocuments(DOCUMENT_DECODER, new Document("_id", 1).append("ts", new BsonTimestamp(5, 0)));

        BsonDocument commandResult = executeFindCommand(new BsonDocument("ts",
                new BsonDocument("$gte", new BsonTimestamp(5, 0))), 0, 2, true, true);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 2, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger seen = new AtomicInteger();
        Thread thread = new Thread(() -> {
            try {
                cursorNext();
                seen.incrementAndGet();
                cursorNext();
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
        BsonDocument commandResult = executeFindCommand(5);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 5, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertNotNull(cursorNext());
        assertTrue(cursor.isClosed());
        assertNull(cursor.getServerCursor());
    }

    @Test
    @DisplayName("should kill cursor if limit is reached on getMore")
    void shouldKillCursorIfLimitIsReachedOnGetMore() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand();
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 5, 3, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        ServerCursor serverCursor = cursor.getServerCursor();
        assertNotNull(serverCursor);
        assertNotNull(cursorNext());
        assertNotNull(cursorNext());

        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connection, 1));
        assertThrows(MongoQueryException.class, () ->
                makeAdditionalGetMoreCall(getNamespace(), serverCursor, connection)
        );
    }

    @Test
    @DisplayName("should release connection source if limit is reached on initial query")
    void shouldReleaseConnectionSourceIfLimitIsReachedOnInitialQuery() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand(5);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 5, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertNull(cursor.getServerCursor());
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connectionSource, 1));
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connection, 1));
    }

    @Test
    @DisplayName("should release connection source if limit is reached on getMore")
    void shouldReleaseConnectionSourceIfLimitIsReachedOnGetMore() {
        assumeFalse(isSharded());
        BsonDocument commandResult = executeFindCommand(3);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 5, 3, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertNotNull(cursorNext());
        assertNotNull(cursorNext());
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connectionSource, 1));
        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connection, 1));
    }

    @Test
    @DisplayName("test limit with get more")
    void testLimitWithGetMore() {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 5, 2, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertNotNull(cursorNext());
        assertNotNull(cursorNext());
        assertNotNull(cursorNext());

        assertDoesNotThrow(() -> checkReferenceCountReachesTarget(connectionSource, 1));
        assertTrue(cursor.isClosed());
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
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 300, 0, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertEquals(300, cursorFlatten().size());
    }

    @Test
    @DisplayName("should respect batch size")
    void shouldRespectBatchSize() {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 2, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        assertEquals(2, cursor.getBatchSize());
        assertEquals(2, cursorNext().size());
        assertEquals(2, cursorNext().size());

        cursor.setBatchSize(3);
        assertEquals(3, cursor.getBatchSize());
        assertEquals(3, cursorNext().size());
        assertEquals(3, cursorNext().size());
    }

    @Test
    @DisplayName("should throw cursor not found exception")
    void shouldThrowCursorNotFoundException() throws Throwable {
        BsonDocument commandResult = executeFindCommand(2);
        cursor = new AsyncCommandBatchCursor<>(getServerAddress(), commandResult, 0, 2, 0, DOCUMENT_DECODER,
                null, connectionSource, connection);

        ServerCursor serverCursor = cursor.getServerCursor();
        assertNotNull(serverCursor);
        AsyncConnection localConnection = getConnection(connectionSource);
        this.<BsonDocument>block(cb -> localConnection.commandAsync(getNamespace().getDatabaseName(),
                new BsonDocument("killCursors", new BsonString(getNamespace().getCollectionName()))
                        .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId())))),
                NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(), connectionSource, cb));
        localConnection.release();

        cursorNext();

        MongoCursorNotFoundException exception = assertThrows(MongoCursorNotFoundException.class, this::cursorNext);
        assertEquals(serverCursor.getId(), exception.getCursorId());
        assertEquals(serverCursor.getAddress(), exception.getServerAddress());
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

    private ServerAddress getServerAddress() {
        return connection.getDescription().getServerAddress();
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

        BsonDocument results = block(cb -> connection.commandAsync(getDatabaseName(), findCommand,
                NO_OP_FIELD_NAME_VALIDATOR, readPreference,
                CommandResultDocumentCodec.create(DOCUMENT_DECODER, "firstBatch"),
                connectionSource, cb));

        assertNotNull(results);
        return results;
    }

    private List<Document> cursorNext() {
        return block(cb -> cursor.next(cb));
    }

    private List<Document> cursorFlatten() {
        List<Document> results = new ArrayList<>();
        while (!cursor.isClosed()) {
            results.addAll(cursorNext());
        }
        return results;
    }
}

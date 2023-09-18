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

package com.mongodb.internal.operation

import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoException
import com.mongodb.MongoQueryException
import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.WriteConcern
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadWriteBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf
import util.spock.annotations.Slow

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getConnection
import static com.mongodb.ClusterFixture.getReferenceCountAfterTimeout
import static com.mongodb.ClusterFixture.getWriteConnectionSource
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.internal.operation.QueryOperationHelper.makeAdditionalGetMoreCall
import static java.util.Collections.singletonList
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class AsyncCommandBatchCursorFunctionalSpecification extends OperationFunctionalSpecification {
    AsyncConnectionSource connectionSource
    AsyncCommandBatchCursor<Document> cursor
    AsyncConnection connection

    def setup() {
        def documents = []
        for (int i = 0; i < 10; i++) {
            documents.add(new BsonDocument('_id', new BsonInt32(i)))
        }
        collectionHelper.insertDocuments(documents,
                isDiscoverableReplicaSet() ? WriteConcern.MAJORITY : WriteConcern.ACKNOWLEDGED,
                getBinding())
        setUpConnectionAndSource(getAsyncBinding())
    }

    private void setUpConnectionAndSource(final AsyncReadWriteBinding binding) {
        connectionSource = getWriteConnectionSource(binding)
        connection = getConnection(connectionSource)
    }

    def cleanup() {
        cursor?.close()
        getReferenceCountAfterTimeout(connectionSource, 1)
        getReferenceCountAfterTimeout(connection, 1)
        cleanupConnectionAndSource()
    }

    private void cleanupConnectionAndSource() {
        connection?.release()
        connectionSource?.release()
    }

    def 'server cursor should not be null'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(2)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        then:
        cursor.getServerCursor() != null
    }


    def 'should get Exceptions for operations on the cursor after closing'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand()

        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        when:
        cursor.close()
        cursor.close()

        and:
        nextBatch()

        then:
        thrown(MongoException)

        and:
        def serverCursor = cursor.getServerCursor()

        then:
        serverCursor == null
    }

    def 'should throw an Exception when going off the end'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(1)
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 2, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        when:
        nextBatch()
        nextBatch()

        then:
        nextBatch().isEmpty()

        when:
        nextBatch()

        then:
        thrown(MongoException)
    }

    def 'test normal exhaustion'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand()

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        then:
        nextBatch().size() == 10
    }

    def 'test limit exhaustion'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(limit, batchSize)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, limit, batchSize, 0, new DocumentCodec(),
                null, connectionSource, connection)
        def batch = nextBatch()
        def counter = batch.size()
        while (!cursor.isClosed()) {
            batch = nextBatch()
            counter += batch == null ? 0 : batch.size()
        }

        then:
        counter == expectedTotal

        where:
        limit | batchSize | expectedTotal
        5     | 2         | 5
        5     | -2        | 2
        -5    | 2         | 5
        -5    | -2        | 5
        2     | 5         | 2
        2     | -5        | 2
        -2    | 5         | 2
        -2    | -5        | 2
    }

    @SuppressWarnings('EmptyCatchBlock')
    def 'should block waiting for next batch on a tailable cursor'() {
        given:
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def (serverAddress, commandResult) = executeFindCommand(new BsonDocument('ts',
                new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, awaitData)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 2, maxTimeMS, new DocumentCodec(),
                null, connectionSource, connection)

        then:
        nextBatch().iterator().next().get('_id') == 1

        when:
        def latch = new CountDownLatch(1)
        Thread.start {
            try {
                sleep(500)
                collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
            } catch (ignored) {
                //pass
            } finally {
                latch.countDown()
            }
        }

        then:
        nextBatch().iterator().next().get('_id') == 2

        cleanup:
        def cleanedUp = latch.await(10, TimeUnit.SECONDS)
        if (!cleanedUp) {
            throw new MongoTimeoutException('Timed out waiting for documents to be inserted')
        }

        where:
        awaitData | maxTimeMS
        true      | 0
        true      | 100
        false     | 0
    }


    @SuppressWarnings('EmptyCatchBlock')
    @Slow
    def 'test tailable interrupt'() throws InterruptedException {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1))

        def (serverAddress, commandResult) = executeFindCommand(new BsonDocument(), 0, 2, true, true)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 2, 0, new DocumentCodec(),
                null, connectionSource, connection)

        CountDownLatch latch = new CountDownLatch(1)
        def seen = 0
        def thread = Thread.start {
            try {
                nextBatch()
                seen = 1
                nextBatch()
                seen = 2
            } catch (ignored) {
                // pass
            } finally {
                latch.countDown()
            }
        }
        sleep(1000)
        thread.interrupt()
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2))
        latch.await()

        then:
        seen == 1
    }

    @IgnoreIf({ isSharded() })
    def 'should kill cursor if limit is reached on initial query'() throws InterruptedException {
        given:
        def (serverAddress, commandResult) = executeFindCommand(5)
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 5, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        when:
        nextBatch()

        then:
        cursor.isClosed()
        cursor.getServerCursor() == null
    }

    @IgnoreIf({ !isStandalone() })
    def 'should kill cursor if limit is reached on get more'() throws InterruptedException {
        given:
        def (serverAddress, commandResult) = executeFindCommand(3)

        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 5, 3, 0, new DocumentCodec(),
                null, connectionSource, connection)
        ServerCursor serverCursor = cursor.getServerCursor()

        nextBatch()
        nextBatch()

        checkReferenceCountReachesTarget(connection, 1)

        when:
        makeAdditionalGetMoreCall(getNamespace(), serverCursor, connection)

        then:
        thrown(MongoQueryException)
    }

    def 'should release connection source if limit is reached on initial query'() throws InterruptedException {
        given:
        def (serverAddress, commandResult) = executeFindCommand(5)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 5, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        then:
        checkReferenceCountReachesTarget(connectionSource, 1)
        checkReferenceCountReachesTarget(connection, 1)
    }

    def 'should release connection source if limit is reached on get more'() throws InterruptedException {
        given:
        def (serverAddress, commandResult) = executeFindCommand(3)

        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 5, 3, 0, new DocumentCodec(),
                null, connectionSource, connection)

        when:
        nextBatch()
        nextBatch()

        then:
        checkReferenceCountReachesTarget(connectionSource, 1)
        checkReferenceCountReachesTarget(connection, 1)
    }

    def 'test limit with get more'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(2)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 5, 2, 0, new DocumentCodec(),
                null, connectionSource, connection)

        then:
        !nextBatch().isEmpty()
        !nextBatch().isEmpty()
        !nextBatch().isEmpty()
        nextBatch().isEmpty()
    }

    @Slow
    def 'test limit with large documents'() {
        given:
        String bigString = new String('x' * 16000)

        (11..1000).each { collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', it).append('s', bigString)) }
        def (serverAddress, commandResult) = executeFindCommand(300, 0)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 300, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)

        def batch = nextBatch()
        def counter = batch.size()
        while (!cursor.isClosed()) {
            batch = nextBatch()
            counter += batch == null ? 0 : batch.size()
        }

        then:
        counter == 300
    }

    def 'should respect batch size'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(2)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 2, 0, new DocumentCodec(),
                null, connectionSource, connection)

        then:
        cursor.batchSize == 2

        nextBatch().size() == 2
        nextBatch().size() == 2

        when:
        cursor.batchSize = 3

        then:
        cursor.batchSize == 3
        nextBatch().size() == 3
        nextBatch().size() == 3
    }

    def 'test normal loop with get more'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(2)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 2, 0, new DocumentCodec(),
                null, connectionSource, connection)
        def batch = nextBatch()
        def counter = batch.size()
        while (!cursor.isClosed()) {
            batch = nextBatch()
            counter += batch == null ? 0 : batch.size()
        }

        then:
        counter == 10
    }


    @SuppressWarnings('BracesForTryCatchFinally')
    @IgnoreIf({ isSharded() })
    def 'should throw cursor not found exception'() {
        given:
        def (serverAddress, commandResult) = executeFindCommand(2)

        when:
        cursor = new AsyncCommandBatchCursor<Document>(serverAddress, commandResult, 0, 2, 0, new DocumentCodec(),
                null, connectionSource, connection)
        def serverCursor = cursor.getServerCursor()

        def callback = new FutureResultCallback<>()
        connection.commandAsync(getNamespace().databaseName,
                new BsonDocument('killCursors', new BsonString(namespace.getCollectionName()))
                        .append('cursors', new BsonArray(singletonList(new BsonInt64(serverCursor.getId())))),
                new NoOpFieldNameValidator(), ReadPreference.primary(), new BsonDocumentCodec(), connectionSource, callback)
        callback.get()
        nextBatch()

        then:
        try {
            nextBatch()
        } catch (MongoCursorNotFoundException e) {
            assertEquals(serverCursor.getId(), e.getCursorId())
            assertEquals(serverCursor.getAddress(), e.getServerAddress())
        } catch (ignored) {
            fail('Expected MongoCursorNotFoundException to be thrown but got ' + ignored.getClass())
        }
    }

    List<Document> nextBatch() {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get()
    }

    private Tuple2<ServerAddress, BsonDocument> executeFindCommand() {
        executeFindCommand(0)
    }

    private Tuple2<ServerAddress, BsonDocument> executeFindCommand(int batchSize) {
        executeFindCommand(new BsonDocument(), 0, batchSize, false, false, ReadPreference.primary())
    }

    private Tuple2<ServerAddress, BsonDocument> executeFindCommand(int limit, int batchSize) {
        executeFindCommand(new BsonDocument(), limit, batchSize, false, false, ReadPreference.primary())
    }

    private Tuple2<ServerAddress, BsonDocument> executeFindCommand(BsonDocument filter, int limit, int batchSize, boolean tailable,
            boolean awaitData) {
        executeFindCommand(filter, limit, batchSize, tailable, awaitData, ReadPreference.primary())
    }

    private Tuple2<ServerAddress, BsonDocument> executeFindCommand(BsonDocument filter, int limit, int batchSize, boolean tailable,
            boolean awaitData, ReadPreference readPreference) {
        def findCommand = new BsonDocument('find', new BsonString(getCollectionName()))
                .append('filter', filter)
                .append('tailable', BsonBoolean.valueOf(tailable))
                .append('awaitData', BsonBoolean.valueOf(awaitData))

        findCommand.append('limit', new BsonInt32(Math.abs(limit)))

        if (limit >= 0) {
            if (batchSize < 0 && Math.abs(batchSize) < limit) {
                findCommand.append('limit', new BsonInt32(Math.abs(batchSize)))
            } else if (batchSize != 0) {
                findCommand.append('batchSize', new BsonInt32(Math.abs(batchSize)))
            }
        }

        def callback = new FutureResultCallback()
        connection.commandAsync(getDatabaseName(), findCommand,
                NO_OP_FIELD_NAME_VALIDATOR, readPreference,
                CommandResultDocumentCodec.create(new DocumentCodec(), 'firstBatch'), connectionSource, callback)
        new Tuple2(connection.getDescription().getServerAddress(), callback.get())
    }
}

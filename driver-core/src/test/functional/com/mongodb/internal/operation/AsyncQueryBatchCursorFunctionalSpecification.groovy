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

import category.Slow
import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerCursor
import com.mongodb.WriteConcern
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.QueryResult
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getConnection
import static com.mongodb.ClusterFixture.getReadConnectionSource
import static com.mongodb.ClusterFixture.getReferenceCountAfterTimeout
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.internal.connection.ServerHelper.waitForLastRelease
import static com.mongodb.internal.connection.ServerHelper.waitForRelease
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class AsyncQueryBatchCursorFunctionalSpecification extends OperationFunctionalSpecification {
    AsyncConnectionSource connectionSource
    AsyncQueryBatchCursor<Document> cursor
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

    private void setUpConnectionAndSource(final AsyncReadBinding binding) {
        connectionSource = getReadConnectionSource(binding)
        connection = getConnection(connectionSource)
    }

    def cleanup() {
        cursor?.close()
        cleanupConnectionAndSource()
    }

    private void cleanupConnectionAndSource() {
        connection?.release()
        connectionSource?.release()
        waitForLastRelease(connectionSource.getServerDescription().getAddress(), getAsyncCluster())
        waitForRelease(connectionSource, 0)
    }

    def 'should exhaust single batch'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(), 0, 0, 0, new DocumentCodec(), connectionSource, connection)

        expect:
        nextBatch().size() == 10
        !nextBatch()
    }

    def 'should not retain connection and source after cursor is exhausted on first batch'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(), 0, 0, 0, new DocumentCodec(), connectionSource, connection)

        when:
        nextBatch()

        then:
        connection.count == 1
        connectionSource.count == 1
    }

    def 'should not retain connection and source after cursor is exhausted on getMore'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(1, 0), 1, 1, 0, new DocumentCodec(), connectionSource, connection)

        when:
        nextBatch()

        then:
        getReferenceCountAfterTimeout(connection, 1) == 1
        getReferenceCountAfterTimeout(connectionSource, 1) == 1
    }

    def 'should not retain connection and source after cursor is exhausted after first batch'() {
        when:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(10, 10), 10, 10, 0, new DocumentCodec(), connectionSource,
                connection)

        then:
        getReferenceCountAfterTimeout(connection, 1) == 1
        getReferenceCountAfterTimeout(connectionSource, 1) == 1
    }

    def 'should exhaust single batch with limit'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(1, 0), 1, 0, 0, new DocumentCodec(), connectionSource, connection)

        expect:
        nextBatch().size() == 1
        !nextBatch()
    }

    def 'should exhaust multiple batches with limit'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(limit, batchSize), limit, batchSize, 0, new DocumentCodec(),
                                                     connectionSource, connection)

        when:
        def next = nextBatch()
        def total = 0
        while (next) {
            total += next.size()
            next = nextBatch()
        }

        then:
        total == expectedTotal

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

    def 'should exhaust multiple batches'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 0, 2, 0, new DocumentCodec(), connectionSource, connection)

        expect:
        nextBatch().size() == 3
        nextBatch().size() == 2
        nextBatch().size() == 2
        nextBatch().size() == 2
        nextBatch().size() == 1
        !nextBatch()
    }

    def 'should respect batch size'() {
        when:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 0, 2, 0, new DocumentCodec(), connectionSource, connection)

        then:
        cursor.batchSize == 2

        when:
        nextBatch()
        cursor.batchSize = 4

        then:
        nextBatch().size() == 4
    }

    def 'should close when exhausted'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(), 0, 2, 0, new DocumentCodec(), connectionSource, connection)

        when:
        cursor.close()
        waitForRelease(connectionSource, 1)

        then:
        connectionSource.count == 1

        when:
        nextBatch()

        then:
        thrown(MongoException)
    }

    def 'should close when not exhausted'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 0, 2, 0, new DocumentCodec(), connectionSource, connection)

        when:
        cursor.close()

        then:
        waitForRelease(connectionSource, 1)
    }

    @Category(Slow)
    def 'should block waiting for first batch on a tailable cursor'() {
        given:
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(4, 0)))
        def firstBatch = executeQuery(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, false);

        when:
        cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 2, 0, new DocumentCodec(), connectionSource, connection)
        def latch = new CountDownLatch(1)
        Thread.start {
            sleep(500)
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(5, 0)))
            latch.countDown()
        }

        def batch = nextBatch()

        then:
        batch.size() == 1
        batch[0].get('_id') == 2

        cleanup:
        def cleanedUp = latch.await(10, SECONDS) // Workaround for codenarc bug
        if (!cleanedUp) {
            throw new MongoTimeoutException('Timed out waiting for documents to be inserted')
        }
    }

    @Category(Slow)
    def 'should block waiting for next batch on a tailable cursor'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQuery(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, awaitData);


        when:
        cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 2, maxTimeMS, new DocumentCodec(), connectionSource, connection)
        def batch = nextBatch()

        then:
        batch.size() == 1
        batch[0].get('_id') == 1

        when:
        def latch = new CountDownLatch(1)
        Thread.start {
            sleep(500)
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
            latch.countDown()
        }

        batch = nextBatch()

        then:
        batch.size() == 1
        batch[0].get('_id') == 2

        cleanup:
        def cleanedUp = latch.await(10, SECONDS)
        if (!cleanedUp) {
            throw new MongoTimeoutException('Timed out waiting for documents to be inserted')
        }

        where:
        awaitData | maxTimeMS
        true      | 0
        true      | 100
        false     | 0
    }

    def 'should respect limit'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(6, 3), 6, 2, 0, new DocumentCodec(), connectionSource, connection)

        expect:
        nextBatch().size() == 3
        nextBatch().size() == 2
        nextBatch().size() == 1
        !nextBatch()
    }

    @IgnoreIf({ isSharded() })
    def 'should kill cursor if limit is reached on initial query'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(5)

        cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 5, 0, 0, new DocumentCodec(), connectionSource, connection)

        when:
        while (connection.getCount() > 1) {
            Thread.sleep(5)
        }
        makeAdditionalGetMoreCall(firstBatch.cursor, connection)

        then:
        thrown(MongoCursorNotFoundException)
    }

    @SuppressWarnings('BracesForTryCatchFinally')
    @IgnoreIf({ isSharded() })
    def 'should throw cursor not found exception'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 2, 0, new DocumentCodec(), connectionSource, connection)

        def latch = new CountDownLatch(1)
        def connection = getConnection(connectionSource)
        def serverCursor = cursor.cursor.get()
        connection.killCursorAsync(getNamespace(), asList(serverCursor.id), new SingleResultCallback<Void>() {
            @Override
            void onResult(final Void result, final Throwable t) {
                latch.countDown()
            }
        })
        latch.await()
        connection.release()
        nextBatch()

        then:
        try {
            nextBatch()
            fail('expected MongoCursorNotFoundException but no exception was thrown')
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
        futureResultCallback.get(60, SECONDS)
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(0)
    }

    private QueryResult<Document> executeQuery(int batchSize) {
        executeQuery(0, batchSize)
    }

    private QueryResult<Document> executeQuery(int limit, int batchSize) {
        executeQuery(new BsonDocument(), limit, batchSize, false, false)
    }

    private QueryResult<Document> executeQuery(BsonDocument filter, int limit, int batchSize, boolean tailable, boolean awaitData) {
        if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
            def findCommand = new BsonDocument('find', new BsonString(getCollectionName()))
                    .append('filter', filter)
                    .append('tailable', BsonBoolean.valueOf(tailable))
                    .append('awaitData', BsonBoolean.valueOf(awaitData))

            findCommand.append('limit', new BsonInt32(Math.abs(limit)))

            if (limit >= 0) {
                if (batchSize < 0 && Math.abs(batchSize) < limit) {
                    findCommand.append('limit', new BsonInt32(Math.abs(batchSize)))
                } else {
                    findCommand.append('batchSize', new BsonInt32(Math.abs(batchSize)))
                }
            }

            def futureResultCallback = new FutureResultCallback<BsonDocument>();
            connection.commandAsync(getDatabaseName(), findCommand, NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(),
                    CommandResultDocumentCodec.create(new DocumentCodec(), 'firstBatch'),
                    connectionSource.sessionContext, futureResultCallback)
            def response = futureResultCallback.get(60, SECONDS)
            cursorDocumentToQueryResult(response.getDocument('cursor'), connection.getDescription().getServerAddress())
        } else {
            def futureResultCallback = new FutureResultCallback<QueryResult<Document>>();
            connection.queryAsync(getNamespace(), filter, null, 0, limit, batchSize,
                                  true, tailable, awaitData, false, false, false,
                                  new DocumentCodec(), futureResultCallback);
            futureResultCallback.get(60, SECONDS);
        }
    }

    private void makeAdditionalGetMoreCall(ServerCursor serverCursor, Connection connection) {
        connection.getMore(getNamespace(), serverCursor.getId(), 1, new DocumentCodec())
    }
}

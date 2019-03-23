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
import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerCursor
import com.mongodb.WriteConcern
import com.mongodb.binding.ConnectionSource
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.Connection
import com.mongodb.connection.QueryResult
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
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo
import static java.util.Arrays.asList
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class QueryBatchCursorFunctionalSpecification extends OperationFunctionalSpecification {
    ConnectionSource connectionSource
    QueryBatchCursor<Document> cursor

    def setup() {
        def documents = []
        for (int i = 0; i < 10; i++) {
            documents.add(new BsonDocument('_id', new BsonInt32(i)))
        }
        collectionHelper.insertDocuments(documents,
                                         isDiscoverableReplicaSet() ? WriteConcern.MAJORITY : WriteConcern.ACKNOWLEDGED,
                                         getBinding())
        connectionSource = getBinding().getReadConnectionSource()
    }

    def cleanup() {
        cursor?.close()
        connectionSource?.release()
    }

    def 'server cursor should not be null'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 0, new DocumentCodec(), connectionSource)

        then:
        cursor.getServerCursor() != null
    }

    def 'test server address'() {
        given:
        def firstBatch = executeQuery()

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 0, new DocumentCodec(), connectionSource)
        then:
        cursor.getServerAddress() != null
    }

    def 'should get Exceptions for operations on the cursor after closing'() {
        given:
        def firstBatch = executeQuery()

        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 0, new DocumentCodec(), connectionSource)

        when:
        cursor.close()
        cursor.close()

        and:
        cursor.next()

        then:
        thrown(IllegalStateException)

        when:
        cursor.hasNext()

        then:
        thrown(IllegalStateException)

        when:
        cursor.getServerCursor()

        then:
        thrown(IllegalStateException)
    }

    def 'should throw an Exception when going off the end'() {
        given:
        def firstBatch = executeQuery(1)

        cursor = new QueryBatchCursor<Document>(firstBatch, 2, 0, new DocumentCodec(), connectionSource)
        when:
        cursor.next()
        cursor.next()
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }

    def 'test normal exhaustion'() {
        given:
        def firstBatch = executeQuery()

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 0, new DocumentCodec(), connectionSource)

        then:
        cursor.iterator().sum { it.size } == 10
    }

    def 'test limit exhaustion'() {
        given:
        def firstBatch = executeQuery(limit, batchSize)
        def connection = connectionSource.getConnection()

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, limit, batchSize, 0, new DocumentCodec(), connectionSource, connection)

        then:
        cursor.iterator().sum { it.size } == expectedTotal

        cleanup:
        connection?.release()

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

    def 'test remove'() {
        given:
        def firstBatch = executeQuery()

        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 0, new DocumentCodec(), connectionSource)

        when:
        cursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    @SuppressWarnings('EmptyCatchBlock')
    @Category(Slow)
    def 'should block waiting for next batch on a tailable cursor'() {
        given:
        def connection = connectionSource.getConnection()
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQuery(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, awaitData);

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, maxTimeMS, new DocumentCodec(), connectionSource, connection)

        then:
        cursor.hasNext()
        cursor.next().iterator().next().get('_id') == 1

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

        // Note: this test is racy.
        // The sleep above does not guarantee that we're testing what we're trying to, which is the loop in the hasNext() method.
        then:
        cursor.hasNext()
        cursor.next().iterator().next().get('_id') == 2

        cleanup:
        def cleanedUp = latch.await(10, TimeUnit.SECONDS)
        if (!cleanedUp) {
            throw new MongoTimeoutException('Timed out waiting for documents to be inserted')
        }
        connection?.release()

        where:
        awaitData | maxTimeMS
        true      | 0
        true      | 100
        false     | 0
    }

    @Category(Slow)
    def 'test try next with tailable'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQuery(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, true);


        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.tryNext().iterator().next().get('_id') == 1

        then:
        !cursor.tryNext()

        when:
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
        def nextBatch = cursor.tryNext()

        then:
        nextBatch
        nextBatch.iterator().next().get('_id') == 2
    }

    @Category(Slow)
    def 'hasNext should throw when cursor is closed in another thread'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQuery(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, true);
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)
        cursor.next()
        def latch = new CountDownLatch(1)

        // wait a second then close the cursor
        new Thread({
            sleep(1000)
            cursor.close()
            latch.countDown()
        } as Runnable).start()

        when:
        cursor.hasNext()

        then:
        thrown(Exception)

        cleanup:
        latch.await(5, TimeUnit.SECONDS)  // wait for cursor.close to complete
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) || isSharded() })
    @Category(Slow)
    def 'test maxTimeMS'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQuery(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 0, 2, true, true);

        def connection = connectionSource.getConnection()
        def maxTimeMS = 10
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, maxTimeMS, new DocumentCodec(), connectionSource, connection)
        cursor.tryNext()
        long startTime = System.currentTimeMillis()

        when:
        def result = cursor.tryNext()

        then:
        result == null
        // RACY TEST: no guarantee assertion will fire within the given timeframe
        System.currentTimeMillis() - startTime < (maxTimeMS + 200)

        cleanup:
        connection?.release()
   }

    @SuppressWarnings('EmptyCatchBlock')
    @Category(Slow)
    def 'test tailable interrupt'() throws InterruptedException {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1))

        def firstBatch = executeQuery(new BsonDocument(), 0, 2, true, true)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        CountDownLatch latch = new CountDownLatch(1)
        def seen = 0
        def thread = Thread.start {
            try {
                cursor.next()
                seen = 1
                cursor.next()
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
        def firstBatch = executeQuery(5)
        def connection = connectionSource.getConnection()

        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 0, 0, new DocumentCodec(), connectionSource, connection)

        when:
        makeAdditionalGetMoreCall(firstBatch.cursor, connection)

        then:
        thrown(MongoCursorNotFoundException)

        cleanup:
        connection?.release()
    }

    @IgnoreIf({ isSharded() })
    @Category(Slow)
    def 'should kill cursor if limit is reached on get more'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(3)

        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 3, new DocumentCodec(), connectionSource)
        ServerCursor serverCursor = cursor.getServerCursor()

        cursor.next()
        cursor.next()

        Thread.sleep(1000) //Note: waiting for some time for killCursor operation to be performed on a server.
        when:
        makeAdditionalGetMoreCall(serverCursor)

        then:
        thrown(MongoCursorNotFoundException)
    }

    def 'should release connection source if limit is reached on initial query'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(5)
        def connection = connectionSource.getConnection()

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 0, 0, new DocumentCodec(), connectionSource, connection)

        then:
        checkReferenceCountReachesTarget(connectionSource, 1)

        cleanup:
        connection?.release()
    }

    def 'should release connection source if limit is reached on get more'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(3)

        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 3, new DocumentCodec(), connectionSource)

        when:
        cursor.next()
        cursor.next()

        then:
        checkReferenceCountReachesTarget(connectionSource, 1)
    }

    def 'test limit with get more'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.next() != null
        cursor.next() != null
        cursor.next() != null
        !cursor.hasNext()
    }

    @Category(Slow)
    def 'test limit with large documents'() {
        given:
        char[] array = 'x' * 16000
        String bigString = new String(array)

        (11..1000).each { collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', it).append('s', bigString)) }
        def firstBatch = executeQuery(300, 0)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 300, 0, new DocumentCodec(), connectionSource)

        then:
        cursor.iterator().sum { it.size } == 300
    }

    def 'should respect batch size'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.batchSize == 2

        when:
        def nextBatch = cursor.next()

        then:
        nextBatch.size() == 2

        when:
        nextBatch = cursor.next()

        then:
        nextBatch.size() == 2

        when:
        cursor.batchSize = 3
        nextBatch = cursor.next()

        then:
        cursor.batchSize == 3
        nextBatch.size() == 3

        when:
        nextBatch = cursor.next()

        then:
        nextBatch.size() == 3
    }

    def 'test normal loop with get more'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)
        def results = cursor.iterator().collectMany { it*.get('_id') }

        then:
        results == (0..9).toList()
        !cursor.hasNext()
    }

    def 'test next without has next with get more'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        then:
        (0..4).each { cursor.next() }
        !cursor.hasNext()
        !cursor.hasNext()

        when:
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }

    @SuppressWarnings('BracesForTryCatchFinally')
    @IgnoreIf({ isSharded() })
    def 'should throw cursor not found exception'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        def connection = connectionSource.getConnection()
        connection.killCursor(getNamespace(), asList(cursor.getServerCursor().id))
        connection.release()
        cursor.next()

        then:
        try {
            cursor.next()
        } catch (MongoCursorNotFoundException e) {
            assertEquals(cursor.getServerCursor().getId(), e.getCursorId())
            assertEquals(cursor.getServerCursor().getAddress(), e.getServerAddress())
        } catch (ignored) {
            fail('Expected MongoCursorNotFoundException to be thrown but got ' + ignored.getClass())
        }
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(0)
    }

    private QueryResult<Document> executeQuery(int batchSize) {
        executeQuery(new BsonDocument(), 0, batchSize, false, false, ReadPreference.primary())
    }

    private QueryResult<Document> executeQuery(int batchSize, ReadPreference readPreference) {
        executeQuery(new BsonDocument(), 0, batchSize, false, false, readPreference)
    }

    private QueryResult<Document> executeQuery(int limit, int batchSize) {
        executeQuery(new BsonDocument(), limit, batchSize, false, false, ReadPreference.primary())
    }


    private QueryResult<Document> executeQuery(BsonDocument filter, int limit, int batchSize, boolean tailable, boolean awaitData) {
        executeQuery(filter, limit, batchSize, tailable, awaitData, ReadPreference.primary())
    }

    private QueryResult<Document> executeQuery(BsonDocument filter, int limit, int batchSize, boolean tailable, boolean awaitData,
                                               ReadPreference readPreference) {
        def connection = connectionSource.getConnection()
        try {
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

                def response = connection.command(getDatabaseName(), findCommand,
                        OperationFunctionalSpecification.NO_OP_FIELD_NAME_VALIDATOR, readPreference,
                        CommandResultDocumentCodec.create(new DocumentCodec(), 'firstBatch'),
                        connectionSource.sessionContext)
                cursorDocumentToQueryResult(response.getDocument('cursor'), connection.getDescription().getServerAddress())
            } else {
                connection.query(getNamespace(), filter, null, 0, limit, batchSize,
                                 readPreference.isSlaveOk(), tailable, awaitData, false, false, false,
                                 new DocumentCodec());
            }
        } finally {
            connection.release();
        }
    }

    private void makeAdditionalGetMoreCall(ServerCursor serverCursor) {
        def connection = connectionSource.getConnection()
        try {
            makeAdditionalGetMoreCall(serverCursor, connection)
        } finally {
            connection.release()
        }
    }

    private void makeAdditionalGetMoreCall(ServerCursor serverCursor, Connection connection) {
        connection.getMore(getNamespace(), serverCursor.getId(), 1, new DocumentCodec())
    }
}

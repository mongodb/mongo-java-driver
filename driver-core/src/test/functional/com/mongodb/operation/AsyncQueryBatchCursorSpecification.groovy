/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation

import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.FutureResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.QueryResult
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ClusterFixture.getConnection
import static com.mongodb.ClusterFixture.getReadConnectionSource
import static com.mongodb.connection.ServerHelper.waitForLastCheckin
import static java.util.concurrent.TimeUnit.SECONDS

class AsyncQueryBatchCursorSpecification extends OperationFunctionalSpecification {
    AsyncConnectionSource connectionSource
    AsyncQueryBatchCursor<Document> cursor

    def setup() {
        for (int i = 0; i < 10; i++) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i))
        }
        connectionSource = getReadConnectionSource(getAsyncBinding())
    }

    def cleanup() {
        cursor?.close()
        waitForLastCheckin(connectionSource.getServerDescription().getAddress(), getAsyncCluster())
    }

    def 'should exhaust single batch'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(), 0, 0, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 10
        !nextBatch()
        connectionSource.count == 1
    }

    def 'should exhaust single batch with limit'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(1), 1, 0, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 1
        !nextBatch()
        connectionSource.count == 1
    }

    def 'should exhaust multiple batches'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 0, 2, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 3
        nextBatch().size() == 2
        nextBatch().size() == 2
        nextBatch().size() == 2
        nextBatch().size() == 1
        !nextBatch()
        connectionSource.count == 1
    }

    def 'should respect batch size'() {
        when:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 0, 2, new DocumentCodec(), connectionSource)

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
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(), 0, 2, new DocumentCodec(), connectionSource)

        when:
        cursor.close()

        then:
        connectionSource.count == 1

        when:
        cursor.next { }

        then:
        thrown(IllegalStateException)
    }

    def 'should close when not exhausted'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 0, 2, new DocumentCodec(), connectionSource)

        when:
        cursor.close()

        then:
        sleep(500) // racy test, but have to wait for the kill cursor to complete asynchronously
        connectionSource.count == 1
    }

    def 'should block waiting for first batch on a tailable cursor'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(4, 0)))
        def firstBatch = executeQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2, true, false);

        when:
        cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)
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

    def 'should block waiting for next batch on a tailable cursor'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2, true, false);


        when:
        cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)
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
    }

    def 'should respect limit'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(executeQuery(3), 6, 2, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 3
        nextBatch().size() == 2
        nextBatch().size() == 1
        !nextBatch()
    }

    List<Document> nextBatch() {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get(60, SECONDS)
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(0)
    }

    private QueryResult<Document> executeQuery(int numToReturn) {
        executeQueryProtocol(new BsonDocument(), numToReturn, false, false)
    }

    private QueryResult<Document> executeQueryProtocol(BsonDocument query, int numberToReturn, boolean tailable, boolean awaitData) {
        def connection = getConnection(connectionSource)
        try {
            def futureResultCallback = new FutureResultCallback<QueryResult<Document>>();
            connection.queryAsync(getNamespace(), query, null, numberToReturn, 0,
                                  false, tailable, awaitData, false, false, false,
                                  new DocumentCodec(), futureResultCallback);
            futureResultCallback.get(60, SECONDS);
        } finally {
            connection.release()
        }
    }
}

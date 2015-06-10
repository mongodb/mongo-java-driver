package com.mongodb.operation

import category.Slow
import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerCursor
import com.mongodb.binding.ConnectionSource
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.QueryResult
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class QueryBatchCursorSpecification extends OperationFunctionalSpecification {
    ConnectionSource connectionSource
    QueryBatchCursor<Document> cursor

    def setup() {
        (0..9).each { collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', it)) }
        connectionSource = getBinding().getReadConnectionSource()
    }

    def cleanup() {
        cursor?.close()
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
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.iterator().sum { it.size } == 5
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
    def 'test tailable'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2, true, true);


        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.hasNext()
        cursor.next().iterator().next().get('_id') == 1

        when:
        def latch = new CountDownLatch(1)
        Thread.start {
            try {
                sleep(1000)
                collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
            } catch (interrupt) {
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
    }

    @Category(Slow)
    def 'test try next with tailable'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2, true, true);


        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.tryNext().iterator().next().get('_id') == 1
        !cursor.tryNext()

        when:
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
        def nextBatch = cursor.tryNext()

        then:
        nextBatch
        nextBatch.iterator().next().get('_id') == 2
    }

    @SuppressWarnings('EmptyCatchBlock')
    @Category(Slow)
    def 'test tailable interrupt'() throws InterruptedException {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1))

        def firstBatch = executeQueryProtocol(new BsonDocument(), 2, true, true)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        CountDownLatch latch = new CountDownLatch(1)
        def seen;
        def thread = Thread.start {
            try {
                cursor.next()
                seen = 1
                cursor.next()
                seen = 2
            } catch (interrupt) {
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

    // 2.2 does not properly detect cursor not found, so ignoring
    @IgnoreIf({ isSharded() && !serverVersionAtLeast([2, 4, 0]) })
    @Category(Slow)
    def 'should kill cursor if limit is reached on initial query'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(5)

        cursor = new QueryBatchCursor<Document>(firstBatch, 5, 0, new DocumentCodec(), connectionSource)

        Thread.sleep(1000) //Note: waiting for some time for killCursor operation to be performed on a server.

        when:
        makeAdditionalGetMoreCall(firstBatch.cursor)

        then:
        thrown(MongoCursorNotFoundException)
    }

    @IgnoreIf({ isSharded() && !serverVersionAtLeast([2, 4, 0]) })
    // 2.2 does not properly detect cursor not found, so ignoring
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
        def firstBatch = executeQuery()

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

    // 2.2 does not properly detect cursor not found, so ignoring
    @SuppressWarnings('BracesForTryCatchFinally')
    @IgnoreIf({ isSharded() && !serverVersionAtLeast([2, 4, 0]) })
    def 'should throw cursor not found exception'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, new DocumentCodec(), connectionSource)

        def connection = connectionSource.getConnection()
        connection.killCursor(asList(cursor.getServerCursor().id))
        connection.release()
        cursor.next()

        then:
        try {
            cursor.next()
        } catch (MongoCursorNotFoundException e) {
            assertEquals(cursor.getServerCursor().getId(), e.getCursorId())
            assertEquals(cursor.getServerCursor().getAddress(), e.getServerAddress())
        } catch (ignored) {
            fail()
        }
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(0)
    }

    private QueryResult<Document> executeQuery(int numToReturn) {
        executeQueryProtocol(new BsonDocument(), numToReturn, false, false)
    }

    private QueryResult<Document> executeQueryProtocol(BsonDocument query, int numberToReturn, boolean tailable, boolean awaitData) {
        def connection = connectionSource.getConnection()
        try {
            connection.query(getNamespace(), query, null, numberToReturn, 0,
                             false, tailable, awaitData, false, false, false,
                             new DocumentCodec());
        } finally {
            connection.release();
        }
    }

    private void makeAdditionalGetMoreCall(ServerCursor serverCursor) {
        def connection = connectionSource.getConnection()
        try {
            connection.getMore(getNamespace(), serverCursor.getId(), 1, new DocumentCodec())
        } finally {
            connection.release()
        }
    }
}

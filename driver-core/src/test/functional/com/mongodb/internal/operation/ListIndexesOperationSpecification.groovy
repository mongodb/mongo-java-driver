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

import category.Async
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.QueryResult
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.bulk.IndexRequest
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static java.util.concurrent.TimeUnit.MILLISECONDS

class ListIndexesOperationSpecification extends OperationFunctionalSpecification {

    def 'should return empty list for nonexistent collection'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())

        when:
        def cursor = operation.execute(getBinding())

        then:
        !cursor.hasNext()
    }

    @Category(Async)
    def 'should return empty list for nonexistent collection asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())

        when:
        AsyncBatchCursor cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get() == null
    }


    def 'should return default index on Collection that exists'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        BatchCursor<Document> indexes = operation.execute(getBinding())

        then:
        def firstBatch = indexes.next()
        firstBatch.size() == 1
        firstBatch[0].name == '_id_'
        !indexes.hasNext()
    }

    @Category(Async)
    def 'should return default index on Collection that exists asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def indexes = callback.get()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should return created indexes on Collection'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('compound', new BsonInt32(1)).append('index', new BsonInt32(-1)))
        new CreateIndexesOperation(namespace, [new IndexRequest(new BsonDocument('unique', new BsonInt32(1))).unique(true)])
                .execute(getBinding())

        when:
        BatchCursor cursor = operation.execute(getBinding())

        then:
        def indexes = cursor.next()
        indexes.size() == 4
        indexes*.name.containsAll(['_id_', 'theField_1', 'compound_1_index_-1', 'unique_1'])
        indexes.find { it.name == 'unique_1' }.unique
        !cursor.hasNext()
    }

    @Category(Async)
    def 'should return created indexes on Collection asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('compound', new BsonInt32(1)).append('index', new BsonInt32(-1)))
        new CreateIndexesOperation(namespace, [new IndexRequest(new BsonDocument('unique', new BsonInt32(1))).unique(true)])
                .execute(getBinding())

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def indexes = callback.get()

        then:
        indexes.size() == 4
        indexes*.name.containsAll(['_id_', 'theField_1', 'compound_1_index_-1', 'unique_1'])
        indexes.find { it.name == 'unique_1' }.unique
    }

    def 'should use the set batchSize of collections'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).batchSize(2)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection2', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection3', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection4', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection5', new BsonInt32(1)))

        when:
        def cursor = operation.execute(getBinding())
        def collections = cursor.next()

        then:
        collections.size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.hasNext()
        cursor.getBatchSize() == 2

        when:
        collections = cursor.next()

        then:
        collections.size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.hasNext()
        cursor.getBatchSize() == 2

        cleanup:
        cursor?.close()
    }

    @Category(Async)
    def 'should use the set batchSize of collections asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).batchSize(2)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection2', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection3', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection4', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('collection5', new BsonInt32(1)))

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get().size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.getBatchSize() == 2

        when:
        callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get().size() <= 2 // pre 3.0 items may be filtered out the batch by the driver
        cursor.getBatchSize() == 2

        cleanup:
        consumeAsyncResults(cursor)
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).maxTime(1000, MILLISECONDS)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).maxTime(1000, MILLISECONDS)
        collectionHelper.createIndex(new BsonDocument('collection1', new BsonInt32(1)))

        enableMaxTimeFailPoint()

        when:
        executeAsync(operation);

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }


    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
        }
        def operation = new ListIndexesOperation(helper.namespace, helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.twoSixConnectionDescription
        1 * connection.query(_, _, _, _, _, _, readPreference.isSlaveOk(), _, _, _, _, _, _) >> helper.queryResult
        1 * connection.release()

        when: '3.0.0'
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.threeZeroConnectionDescription
        1 * connection.command(_, _, _, readPreference, _, _) >> helper.commandResult
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadPreference() >> readPreference
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new ListIndexesOperation(helper.namespace, helper.decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.twoSixConnectionDescription
        1 * connection.queryAsync(_, _, _, _, _, _, readPreference.isSlaveOk(), _, _, _, _, _, _, _) >> {
            it[13].onResult(helper.queryResult, null) }

        when: '3.0.0'
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.threeZeroConnectionDescription
        1 * connection.commandAsync(helper.dbName, _, _, readPreference, _, _, _) >> { it[6].onResult(helper.commandResult, null) }

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def helper = [
        dbName: 'db',
        namespace: new MongoNamespace('db', 'coll'),
        decoder: Stub(Decoder),
        twoSixConnectionDescription : Stub(ConnectionDescription) {
            getMaxWireVersion() >> 2
        },
        threeZeroConnectionDescription : Stub(ConnectionDescription) {
            getMaxWireVersion() >> 3
        },
        queryResult: Stub(QueryResult) {
            getNamespace() >> new MongoNamespace('db', 'coll')
            getResults() >> []
            getCursor() >> new ServerCursor(1, Stub(ServerAddress))
        },
        commandResult: new BsonDocument('ok', new BsonDouble(1.0))
                .append('cursor', new BsonDocument('id', new BsonInt64(1)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([])))
    ]
}

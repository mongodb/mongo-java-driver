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


import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.ConnectionDescription
import com.mongodb.internal.async.AsyncBatchCursor
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.QueryResult
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt64
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

class ListCollectionsOperationSpecification extends OperationFunctionalSpecification {

    def madeUpDatabase = 'MadeUpDatabase'

    def 'should return empty set if database does not exist'() {
        given:
        def operation = new ListCollectionsOperation(madeUpDatabase, new DocumentCodec())

        when:
        def cursor = operation.execute(getBinding())

        then:
        !cursor.hasNext()

        cleanup:
        collectionHelper.dropDatabase(madeUpDatabase)
    }


    def 'should return empty cursor if database does not exist asynchronously'() {
        given:
        def operation = new ListCollectionsOperation(madeUpDatabase, new DocumentCodec())

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)

        then:
        callback.get() == null

        when:
        cursor = executeAsync(operation)
        callback = new FutureResultCallback()
        cursor.tryNext(callback)

        then:
        callback.get() == null

        cleanup:
        collectionHelper.dropDatabase(madeUpDatabase)
    }

    def 'should return collection names if a collection exists'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        def cursor = operation.execute(getBinding())
        def collections = cursor.next()
        def names = collections*.get('name')

        then:
        names.containsAll([collectionName, 'collection2'])
        !names.contains(null)
        names.findAll { it.contains('$') }.isEmpty()
    }

    @IgnoreIf({ serverVersionAtLeast(3, 0) })
    def 'should throw if filtering on name with something other than a string'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
                .filter(new BsonDocument('name', new BsonRegularExpression('^[^$]*$')))

        when:
        operation.execute(getBinding())

        then:
        thrown(IllegalArgumentException)
    }

    def 'should filter collection names if a name filter is specified'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
                .filter(new BsonDocument('name', new BsonString('collection2')))
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        def cursor = operation.execute(getBinding())
        def collections = cursor.next()
        def names = collections*.get('name')

        then:
        names.contains('collection2')
        !names.contains(collectionName)
    }

    def 'should filter capped collections'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
                .filter(new BsonDocument('options.capped', BsonBoolean.TRUE))
        def helper = getCollectionHelper()
        getCollectionHelper().create('collection3', new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)

        when:
        def cursor = operation.execute(getBinding())
        def collections = cursor.next()
        def names = collections*.get('name')

        then:
        names.contains('collection3')
        !names.contains(collectionName)
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || serverVersionAtLeast(4, 0) })
    def 'should get all fields when nameOnly is not requested'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
        getCollectionHelper().create('collection4', new CreateCollectionOptions())

        when:
        def cursor = operation.execute(getBinding())
        def collection = cursor.next()[0]

        then:
        collection.size() > 2
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 0) })
    def 'should only get collection names when nameOnly is requested'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
                .nameOnly(true)
        getCollectionHelper().create('collection5', new CreateCollectionOptions())

        when:
        def cursor = operation.execute(getBinding())
        def collection = cursor.next()[0]

        then:
        collection.size() == 2
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || serverVersionAtLeast(4, 0) })
    def 'should only get all field names when nameOnly is requested on server versions that do not support nameOnly'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
                .nameOnly(true)
        getCollectionHelper().create('collection6', new CreateCollectionOptions())

        when:
        def cursor = operation.execute(getBinding())
        def collection = cursor.next()[0]

        then:
        collection.size() > 2
    }


    def 'should return collection names if a collection exists asynchronously'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec())
        def helper = getCollectionHelper()
        def helper2 = getCollectionHelper(new MongoNamespace(databaseName, 'collection2'))
        def codec = new DocumentCodec()
        helper.insertDocuments(codec, ['a': 1] as Document)
        helper2.insertDocuments(codec, ['a': 1] as Document)

        when:
        def cursor = executeAsync(operation)
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def names = callback.get()*.get('name')

        then:
        names.containsAll([collectionName, 'collection2'])
        !names.contains(null)
        names.findAll { it.contains('$') }.isEmpty()
    }

    def 'should filter indexes when calling hasNext before next'() {
        given:
        new DropDatabaseOperation(databaseName).execute(getBinding())
        addSeveralIndexes()
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)

        when:
        def cursor = operation.execute(getBinding())

        then:
        cursor.hasNext()
        cursor.hasNext()
        cursorToListWithNext(cursor)*.get('name').contains(collectionName)
        !cursor.hasNext()
    }

    def 'should filter indexes without calling hasNext before next'() {
        given:
        new DropDatabaseOperation(databaseName).execute(getBinding())
        addSeveralIndexes()
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)

        when:
        def cursor = operation.execute(getBinding())
        def list = cursorToListWithNext(cursor)

        then:
        list*.get('name').contains(collectionName)
        list.findAll { collection -> collection.get('name').contains('$') } == []
        !cursor.hasNext()

        when:
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }

    def 'should filter indexes when calling hasNext before tryNext'() {
        given:
        new DropDatabaseOperation(databaseName).execute(getBinding())
        addSeveralIndexes()
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)

        when:
        def cursor = operation.execute(getBinding())

        then:
        cursor.hasNext()
        cursor.hasNext()

        def list = cursorToListWithTryNext(cursor)
        list*.get('name').contains(collectionName)
        list.findAll { collection -> collection.get('name').contains('$') } == []

        !cursor.hasNext()
        !cursor.hasNext()
        cursor.tryNext() == null
    }

    def 'should filter indexes without calling hasNext before tryNext'() {
        given:
        new DropDatabaseOperation(databaseName).execute(getBinding())
        addSeveralIndexes()
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)

        when:
        def cursor = operation.execute(getBinding())
        def list = cursorToListWithTryNext(cursor)

        then:
        list*.get('name').contains(collectionName)
        list.findAll { collection -> collection.get('name').contains('$') } == []
        cursor.tryNext() == null
    }


    def 'should filter indexes asynchronously'() {
        given:
        new DropDatabaseOperation(databaseName).execute(getBinding())
        addSeveralIndexes()
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)

        when:
        def cursor = executeAsync(operation)
        def list = asyncCursorToList(cursor)

        then:
        list*.get('name').contains(collectionName)
        list.findAll { collection -> collection.get('name').contains('$') } == []
    }

    def 'should use the set batchSize of collections'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)
        def codec = new DocumentCodec()
        getCollectionHelper().insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection2')).insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection3')).insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection4')).insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection5')).insertDocuments(codec, ['a': 1] as Document)

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


    def 'should use the set batchSize of collections asynchronously'() {
        given:
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).batchSize(2)
        def codec = new DocumentCodec()
        getCollectionHelper().insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection2')).insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection3')).insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection4')).insertDocuments(codec, ['a': 1] as Document)
        getCollectionHelper(new MongoNamespace(databaseName, 'collection5')).insertDocuments(codec, ['a': 1] as Document)

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
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }


    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListCollectionsOperation(databaseName, new DocumentCodec()).maxTime(1000, MILLISECONDS)

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
        def operation = new ListCollectionsOperation(helper.dbName, helper.decoder)

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
        def operation = new ListCollectionsOperation(helper.dbName, helper.decoder)

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

    private void addSeveralIndexes() {
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions())
        getCollectionHelper().createIndex(['a': 1] as Document)
        getCollectionHelper().createIndex(['b': 1] as Document)
        getCollectionHelper().createIndex(['c': 1] as Document)
        getCollectionHelper().createIndex(['d': 1] as Document)
        getCollectionHelper().createIndex(['e': 1] as Document)
        getCollectionHelper().createIndex(['f': 1] as Document)
        getCollectionHelper().createIndex(['g': 1] as Document)
    }

    def cursorToListWithNext(BatchCursor cursor) {
        def list = []
        while (cursor.hasNext()) {
            list += cursor.next()
        }
        list
    }

    def cursorToListWithTryNext(BatchCursor cursor) {
        def list = []
        while (true) {
            def next = cursor.tryNext()
            if (next == null) {
                break;
            }
            list += next
        }
        list
    }

    def asyncCursorToList(AsyncBatchCursor cursor) {
        def callback = new FutureResultCallback()
        cursor.next(callback)
        def next = callback.get();
        if (next == null) {
            return []
        }

        next + asyncCursorToList(cursor)
    }
}

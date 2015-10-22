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

import category.Async
import com.mongodb.Block
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.client.test.CollectionHelper
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.loopCursor
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MapReduceWithInlineResultsOperationSpecification extends OperationFunctionalSpecification {
    private final documentCodec = new DocumentCodec()
    def mapReduceOperation = new MapReduceWithInlineResultsOperation<Document>(
            getNamespace(),
            new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
            new BsonJavaScript('function(key, values){ return values.length; }'),
            documentCodec)

    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]

    def setup() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should have the correct defaults'() {
        when:
        def mapF = new BsonJavaScript('function(){ }')
        def reduceF = new BsonJavaScript('function(key, values){ }')
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, mapF, reduceF, helper.decoder)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getFilter() == null
        operation.getFinalizeFunction() == null
        operation.getScope() == null
        operation.getSort() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getLimit() == 0
        operation.getReadConcern() == ReadConcern.DEFAULT
        !operation.isJsMode()
        !operation.isVerbose()
    }

    def 'should set optional values correctly'(){
        when:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def scope = new BsonDocument('scope', new BsonInt32(1))
        def sort = new BsonDocument('sort', new BsonInt32(1))
        def finalizeF = new BsonJavaScript('function(key, value){}')
        def mapF = new BsonJavaScript('function(){ }')
        def reduceF = new BsonJavaScript('function(key, values){ }')
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, mapF, reduceF, helper.decoder)
                .filter(filter)
                .finalizeFunction(finalizeF)
                .scope(scope)
                .sort(sort)
                .jsMode(true)
                .verbose(true)
                .limit(20)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getFilter() == filter
        operation.getFinalizeFunction() == finalizeF
        operation.getScope() == scope
        operation.getSort() == sort
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getLimit() == 20
        operation.getReadConcern() == ReadConcern.MAJORITY
        operation.isJsMode()
        operation.isVerbose()
    }

    def 'should return the correct results'() {
        when:
        MapReduceBatchCursor<Document> results = mapReduceOperation.execute(getBinding())

        then:
        results.iterator().next() == expectedResults
    }

    @Category(Async)
    def 'should return the correct results asynchronously'() {
        when:
        List<Document> docList = []
        loopCursor(mapReduceOperation, new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        });

        then:
        docList.iterator().toList() == expectedResults
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
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.command(helper.dbName, _, readPreference.isSlaveOk(), _, _) >> helper.commandResult
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
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.commandAsync(helper.dbName, _, readPreference.isSlaveOk(), _, _, _) >> {
            it[5].onResult(helper.commandResult, null) }
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should create the expected command'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
        }
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder)
        def expectedCommand = new BsonDocument('mapreduce', new BsonString(helper.namespace.getCollectionName()))
            .append('map', operation.getMapFunction())
            .append('reduce', operation.getReduceFunction())
            .append('out', new BsonDocument('inline', new BsonInt32(1)))
            .append('query', new BsonNull())
            .append('sort', new BsonNull())
            .append('finalize', new BsonNull())
            .append('scope', new BsonNull())
            .append('verbose', BsonBoolean.FALSE)

        when:
        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, expectedCommand, _, _, _) >> { helper.commandResult }
        1 * connection.release()

        when:
        operation.filter(new BsonDocument('filter', new BsonInt32(1)))
                .scope(new BsonDocument('scope', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('function(key, value){}'))
                .jsMode(true)
                .verbose(true)
                .limit(20)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        expectedCommand.append('query', operation.getFilter())
                .append('scope', operation.getScope())
                .append('sort', operation.getSort())
                .append('finalize', operation.getFinalizeFunction())
                .append('jsMode', BsonBoolean.TRUE)
                .append('verbose', BsonBoolean.TRUE)
                .append('maxTimeMS', new BsonInt64(10))
                .append('limit', new BsonInt32(20))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, _, _, _, _) >> { helper.commandResult }
        1 * connection.release()
    }

    def 'should create the expected command asynchronously'() {
        given:
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder)
        def expectedCommand = new BsonDocument('mapreduce', new BsonString(helper.namespace.getCollectionName()))
                .append('map', operation.getMapFunction())
                .append('reduce', operation.getReduceFunction())
                .append('out', new BsonDocument('inline', new BsonInt32(1)))
                .append('query', new BsonNull())
                .append('sort', new BsonNull())
                .append('finalize', new BsonNull())
                .append('scope', new BsonNull())
                .append('verbose', BsonBoolean.FALSE)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()

        when:
        operation.filter(new BsonDocument('filter', new BsonInt32(1)))
                .scope(new BsonDocument('scope', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('function(key, value){}'))
                .jsMode(true)
                .verbose(true)
                .limit(20)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        expectedCommand.append('query', operation.getFilter())
                .append('scope', operation.getScope())
                .append('sort', operation.getSort())
                .append('finalize', operation.getFinalizeFunction())
                .append('jsMode', BsonBoolean.TRUE)
                .append('verbose', BsonBoolean.TRUE)
                .append('maxTimeMS', new BsonInt64(10))
                .append('limit', new BsonInt32(20))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()
    }

    def 'should validate the ReadConcern'() {
        given:
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> Stub(ConnectionSource) {
                getConnection() >> Stub(Connection) {
                    getDescription() >> Stub(ConnectionDescription) {
                        getServerVersion() >> new ServerVersion([3, 0, 0])
                    }
                }
            }
        }
        def operation = new CountOperation(helper.namespace).readConcern(readConcern)

        when:
        operation.execute(readBinding)

        then:
        thrown(IllegalArgumentException)

        where:
        readConcern << [ReadConcern.MAJORITY, ReadConcern.LOCAL]
    }

    def 'should validate the ReadConcern asynchronously'() {
        given:
        def connection = Stub(AsyncConnection) {
            getDescription() >>  Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 0, 0])
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new CountOperation(helper.namespace).readConcern(readConcern)
        def callback = Mock(SingleResultCallback)

        when:
        operation.executeAsync(readBinding, callback)

        then:
        1 * callback.onResult(null, _ as IllegalArgumentException)

        where:
        readConcern << [ReadConcern.MAJORITY, ReadConcern.LOCAL]
    }

    def helper = [
            dbName: 'db',
            namespace: new MongoNamespace('db', 'coll'),
            decoder: Stub(Decoder),
            commandResult: BsonDocument.parse('{ok: 1.0, counts: {input: 1, emit: 1, output: 1}, timeMillis: 1}')
                    .append('results', new BsonArrayWrapper([])),
            connectionDescription: Stub(ConnectionDescription)
    ]
}

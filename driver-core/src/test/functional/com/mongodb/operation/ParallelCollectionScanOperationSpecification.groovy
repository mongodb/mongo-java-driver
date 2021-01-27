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

package com.mongodb.operation

import category.Async
import category.Slow
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
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import com.mongodb.session.SessionContext
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.ConcurrentHashMap

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.loopCursor
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static org.junit.Assert.assertTrue

@IgnoreIf({ isSharded() || serverVersionAtLeast(4, 2) } )
@Category(Slow)
class ParallelCollectionScanOperationSpecification extends OperationFunctionalSpecification {
    Map<Integer, Boolean> ids = [] as ConcurrentHashMap

    def 'setup'() {
        (1..2000).each {
            ids.put(it, true)
        }

        getCollectionHelper().insertDocuments(new DocumentCodec(), (1..2000).collect( { new Document('_id', it) } ))
    }


    def 'should have the correct defaults'() {
        when:
        def operation = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())

        then:
        operation.getBatchSize() == 0
        operation.getNumCursors() == 3
    }

    def 'should set optional values correctly'(){
        when:
        def operation = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
            .batchSize(10)

        then:
        operation.getBatchSize() == 10
        operation.getNumCursors() == 3
    }

    def 'should visit all documents'() {
        when:
        def cursors = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
                .batchSize(500).execute(getBinding())

        then:
        cursors.size() <= 3

        when:
        cursors.each { batchCursor -> batchCursor.each { cursor -> cursor.each { doc -> ids.remove(doc.getInteger('_id')) } } }

        then:
        ids.isEmpty()
    }

    @Category(Async)
    def 'should visit all documents asynchronously'() {
        when:
        def cursors = executeAsync(new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec()).batchSize(500))

        then:
        cursors.size() <= 3

        when:
        loopCursor(cursors, new Block<Document>() {
            @Override
            void apply(final Document document) {
                assertTrue(ids.remove((Integer) document.get('_id')))
            }
        })

        then:
        ids.isEmpty()
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
            getSessionContext() >> Stub(SessionContext) {
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }
        def operation = new ParallelCollectionScanOperation<Document>(helper.namespace, 2, helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.command(helper.dbName, _, _, readPreference, _, _) >> helper.commandResult
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
            getSessionContext() >> Stub(SessionContext) {
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }
        def operation = new ParallelCollectionScanOperation<Document>(helper.namespace, 2, helper.decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.commandAsync(helper.dbName, _, _, readPreference, _, _, _) >> {
            it[6].onResult(helper.commandResult, null) }
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
            getSessionContext() >> Stub(SessionContext) {
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }

        def operation = new ParallelCollectionScanOperation<Document>(helper.namespace, 2, helper.decoder)
        def expectedCommand = new BsonDocument('parallelCollectionScan', new BsonString(helper.namespace.getCollectionName()))
                .append('numCursors', new BsonInt32(2))

        when:
        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, expectedCommand, _, _, _, _) >> { helper.commandResult }
        1 * connection.release()

        when:
        operation.batchSize(10)

        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, expectedCommand, _, _, _, _) >> { helper.commandResult }
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
            getSessionContext() >> Stub(SessionContext) {
                hasActiveTransaction() >> false
                getReadConcern() >> ReadConcern.DEFAULT
            }
        }
        def operation = new ParallelCollectionScanOperation<Document>(helper.namespace, 2, helper.decoder)
        def expectedCommand = new BsonDocument('parallelCollectionScan', new BsonString(helper.namespace.getCollectionName()))
                .append('numCursors', new BsonInt32(2))

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _, _) >> { it[6].onResult(helper.commandResult, null) }
        1 * connection.release()

        when:
        operation.batchSize(10)

        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _, _) >> { it[6].onResult(helper.commandResult, null) }
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
            getSessionContext() >> Stub(SessionContext) {
                getReadConcern() >> readConcern
            }
        }
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder)

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
            getSessionContext() >> Stub(SessionContext) {
                getReadConcern() >> readConcern
            }
        }
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder)
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
            commandResult: BsonDocument.parse('{ok: 1.0, cursors: []}'),
            connectionDescription: Stub(ConnectionDescription)
    ]
}

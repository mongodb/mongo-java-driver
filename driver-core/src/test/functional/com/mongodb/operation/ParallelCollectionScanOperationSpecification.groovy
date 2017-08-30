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
import category.Slow
import com.mongodb.Block
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerVersion
import com.mongodb.connection.SessionContext
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.BsonTimestamp
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
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.operation.ReadConcernHelper.appendReadConcernToCommand
import static org.junit.Assert.assertTrue

@IgnoreIf({ isSharded() })
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
        operation.getReadConcern() == ReadConcern.DEFAULT
    }

    def 'should set optional values correctly'(){
        when:
        def operation = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
            .batchSize(10)
            .readConcern(ReadConcern.MAJORITY)

        then:
        operation.getBatchSize() == 10
        operation.getNumCursors() == 3
        operation.getReadConcern() == ReadConcern.MAJORITY
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
        }
        def operation = new ParallelCollectionScanOperation<Document>(helper.namespace, 2, helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.command(helper.dbName, _, readPreference, _, _, _) >> helper.commandResult
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
        def operation = new ParallelCollectionScanOperation<Document>(helper.namespace, 2, helper.decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.commandAsync(helper.dbName, _, readPreference, _, _, _, _) >> {
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
        operation.batchSize(10).readConcern(ReadConcern.MAJORITY)
        expectedCommand.append('readConcern', new BsonDocument('level', new BsonString('majority')))

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
        operation.batchSize(10).readConcern(ReadConcern.MAJORITY)
        expectedCommand.append('readConcern', new BsonDocument('level', new BsonString('majority')))

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
        }
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder).readConcern(readConcern)

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
        def operation = new MapReduceWithInlineResultsOperation<Document>(helper.namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), helper.decoder).readConcern(readConcern)
        def callback = Mock(SingleResultCallback)

        when:
        operation.executeAsync(readBinding, callback)

        then:
        1 * callback.onResult(null, _ as IllegalArgumentException)

        where:
        readConcern << [ReadConcern.MAJORITY, ReadConcern.LOCAL]
    }

    def 'should add read concern to command'() {
        given:
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.readConnectionSource >> source
        binding.sessionContext >> sessionContext
        source.connection >> connection
        source.retain() >> source
        def commandDocument = new BsonDocument('parallelCollectionScan', new BsonString(getCollectionName()))
            .append('numCursors', new BsonInt32(1))
        appendReadConcernToCommand(ReadConcern.MAJORITY, sessionContext, commandDocument)

        def operation = new ParallelCollectionScanOperation<Document>(getNamespace(), 1, new DocumentCodec())
                .readConcern(ReadConcern.MAJORITY)

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 6), STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, sessionContext) >>
                new BsonDocument('cursors', new BsonArray())
        1 * connection.release()

        where:
        sessionContext << [
                Stub(SessionContext) {
                    isCausallyConsistent() >> true
                    getOperationTime() >> new BsonTimestamp(42, 0)
                }
        ]
    }

    def 'should add read concern to command asynchronously'() {
        given:
        def binding = Stub(AsyncReadBinding)
        def source = Stub(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        binding.readPreference >> ReadPreference.primary()
        binding.getReadConnectionSource(_) >> { it[0].onResult(source, null) }
        binding.sessionContext >> sessionContext
        source.getConnection(_) >> { it[0].onResult(connection, null) }
        source.retain() >> source
        def commandDocument = new BsonDocument('parallelCollectionScan', new BsonString(getCollectionName()))
                .append('numCursors', new BsonInt32(1))
        appendReadConcernToCommand(ReadConcern.MAJORITY, sessionContext, commandDocument)

        def operation = new ParallelCollectionScanOperation<Document>(getNamespace(), 1, new DocumentCodec())
                .readConcern(ReadConcern.MAJORITY)

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 6), STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, sessionContext, _) >> {
            it[6].onResult(new BsonDocument('cursors', new BsonArray()), null)
        }
        1 * connection.release()

        where:
        sessionContext << [
                Stub(SessionContext) {
                    isCausallyConsistent() >> true
                    getOperationTime() >> new BsonTimestamp(42, 0)
                }
        ]
    }

    def helper = [
            dbName: 'db',
            namespace: new MongoNamespace('db', 'coll'),
            decoder: Stub(Decoder),
            commandResult: BsonDocument.parse('{ok: 1.0, cursors: []}'),
            connectionDescription: Stub(ConnectionDescription)
    ]
}

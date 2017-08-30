/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.connection

import com.mongodb.MongoNamespace
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernResult
import com.mongodb.async.SingleResultCallback
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.diagnostics.logging.Logger
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared
import spock.lang.Specification

import static com.mongodb.CustomMatchers.compare
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.connection.ServerType.SHARD_ROUTER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback
import static java.util.Arrays.asList

class DefaultServerConnectionSpecification extends Specification {
    def namespace = new MongoNamespace('test', 'test')
    def internalConnection = Mock(InternalConnection)
    def callback = errorHandlingCallback(Mock(SingleResultCallback), Mock(Logger))
    @Shared
    def standaloneConnectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            new ServerVersion(3, 0), STANDALONE, 100, 100, 100, [])
    @Shared
    def mongosConnectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            new ServerVersion(3, 0), SHARD_ROUTER, 100, 100, 100, [])

    def 'should execute insert protocol'() {
        given:
        def inserts = asList(new InsertRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new InsertProtocol(namespace, true, UNACKNOWLEDGED, inserts), it) }, internalConnection) >> {
                WriteConcernResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.insert(namespace, true, UNACKNOWLEDGED, inserts)

        then:
        result == WriteConcernResult.unacknowledged()
    }

    def 'should execute update protocol'() {
        given:
        def updates = asList(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE))
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new UpdateProtocol(namespace, true, UNACKNOWLEDGED, updates), it) }, internalConnection) >> {
                WriteConcernResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.update(namespace, true, UNACKNOWLEDGED, updates)

        then:
        result == WriteConcernResult.unacknowledged()
    }

    def 'should execute delete protocol'() {
        given:
        def deletes = asList(new DeleteRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new DeleteProtocol(namespace, true, UNACKNOWLEDGED, deletes), it) }, internalConnection) >> {
                WriteConcernResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.delete(namespace, true, UNACKNOWLEDGED, deletes)

        then:
        result == WriteConcernResult.unacknowledged()
    }

    def 'should execute insert command protocol'() {
        given:
        def inserts = asList(new InsertRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new InsertCommandProtocol(namespace, true, UNACKNOWLEDGED, null, inserts), it) },
                    internalConnection, NoOpSessionContext.INSTANCE) >> { (BulkWriteResult.unacknowledged())
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.insertCommand(namespace, true, UNACKNOWLEDGED, inserts)

        then:
        result == BulkWriteResult.unacknowledged()
    }

    def 'should execute update command protocol'() {
        given:
        def updates = asList(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE))
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new UpdateCommandProtocol(namespace, true, UNACKNOWLEDGED, null, updates), it) },
                    internalConnection, NoOpSessionContext.INSTANCE) >> {
                BulkWriteResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.updateCommand(namespace, true, ACKNOWLEDGED, updates)

        then:
        result == BulkWriteResult.unacknowledged()
    }

    def 'should execute delete command protocol'() {
        given:
        def deletes = asList(new DeleteRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new DeleteCommandProtocol(namespace, true, UNACKNOWLEDGED, deletes), it) },
                    internalConnection, NoOpSessionContext.INSTANCE) >> {
                BulkWriteResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.deleteCommand(namespace, true, ACKNOWLEDGED, deletes)

        then:
        result == BulkWriteResult.unacknowledged()
    }

    def 'should execute command protocol with slaveok'() {
        given:
        def command = new BsonDocument('ismaster', new BsonInt32(1))
        def validator = new NoOpFieldNameValidator()
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new SimpleCommandProtocol('test', command, validator, codec).readPreference(expectedReadPreference), it) },
                    internalConnection, NoOpSessionContext.INSTANCE) >> {
                new BsonDocument()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.command('test', command, slaveOk, validator, codec)

        then:
        result == new BsonDocument()

        where:
        slaveOk  | expectedReadPreference
        true     | ReadPreference.secondaryPreferred()
        false    | ReadPreference.primary()
    }

    def 'should set slaveOk when executing command protocol on connection in SINGLE connection mode'() {
        given:
        def command = new BsonDocument('ismaster', new BsonInt32(1))
        def validator = new NoOpFieldNameValidator()
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new SimpleCommandProtocol('test', command, validator, codec).readPreference(expectedReadPreference), it) },
                    internalConnection, NoOpSessionContext.INSTANCE) >> {
                new BsonDocument()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.SINGLE)
        internalConnection.description >> connectionDescription

        when:
        def result = connection.command('test', command, false, validator, codec)

        then:
        result == new BsonDocument()

        where:
        connectionDescription           | expectedSlaveOk | expectedReadPreference
        standaloneConnectionDescription | true            | ReadPreference.secondaryPreferred()
        mongosConnectionDescription     | false           | ReadPreference.primary()
    }

    def 'should execute query protocol'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def expectedResult = new QueryResult<>(namespace, [], 0, new ServerAddress())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new QueryProtocol(namespace, 2, 1, query, fields, decoder)
                    .slaveOk(slaveOk)
                    .tailableCursor(false)
                    .awaitData(true)
                    .noCursorTimeout(false)
                    .partial(true)
                    .oplogReplay(false), it) }, internalConnection) >> {
                expectedResult
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.query(namespace, query, fields, 1, 2, slaveOk, false, true, false, true, false, decoder)

        then:
        result == expectedResult

        where:
        slaveOk << [true, false]
    }

    def 'should set slaveOk when executing query protocol on connection in SINGLE connection mode'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def expectedResult = new QueryResult<>(namespace, [], 0, new ServerAddress())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new QueryProtocol(namespace, 2, 1, query, fields, decoder)
                    .slaveOk(expectedSlaveOk)
                    .tailableCursor(false)
                    .awaitData(true)
                    .noCursorTimeout(false)
                    .partial(true)
                    .oplogReplay(false), it) }, internalConnection) >> {
                expectedResult
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.SINGLE)
        internalConnection.description >> connectionDescription

        when:
        def result = connection.query(namespace, query, fields, 1, 2, false, false, true, false, true, false, decoder)

        then:
        result == expectedResult

        where:
        connectionDescription           | expectedSlaveOk
        standaloneConnectionDescription | true
        mongosConnectionDescription     | false
    }

    def 'should execute getmore protocol'() {
        given:
        def codec = new BsonDocumentCodec()
        def expectedResult = new QueryResult<>(namespace, [], 0, new ServerAddress())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new GetMoreProtocol(namespace, 1000L, 1, codec), it) }, internalConnection) >> {
                expectedResult
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.getMore(namespace, 1000L, 1, codec)

        then:
        result == expectedResult
    }

    def 'should execute kill cursor protocol'() {
        given:
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.killCursor([5])

        then:
        1 * executor.execute({ compare(new KillCursorProtocol(null, [5]), it) }, internalConnection)

        when:
        connection.killCursor(namespace, [5])

        then:
        1 * executor.execute({ compare(new KillCursorProtocol(namespace, [5]), it) }, internalConnection)
    }

    def 'should execute insert protocol asynchronously'() {
        given:
        def inserts = asList(new InsertRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.insertAsync(namespace, true, UNACKNOWLEDGED, inserts, callback)

        then:
        1 * executor.executeAsync({ compare(new InsertProtocol(namespace, true, UNACKNOWLEDGED, inserts), it) }, internalConnection,
                callback)
    }

    def 'should execute update protocol asynchronously'() {
        given:
        def updates = asList(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.updateAsync(namespace, true, UNACKNOWLEDGED, updates, callback)

        then:
        1 * executor.executeAsync({ compare(new UpdateProtocol(namespace, true, UNACKNOWLEDGED, updates), it) },
                                  internalConnection, callback)
    }

    def 'should execute delete protocol asynchronously'() {
        given:
        def deletes = asList(new DeleteRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor)

        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.deleteAsync(namespace, true, UNACKNOWLEDGED, deletes, callback)

        then:
        1 * executor.executeAsync({ compare(new DeleteProtocol(namespace, true, UNACKNOWLEDGED, deletes), it) }, internalConnection,
                callback)
    }

    def 'should execute insert command protocol asynchronously'() {
        given:
        def inserts = asList(new InsertRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.insertCommandAsync(namespace, true, ACKNOWLEDGED, inserts, callback)

        then:
        1 * executor.executeAsync({ compare(new InsertCommandProtocol(namespace, true, ACKNOWLEDGED, null, inserts), it) },
                                  internalConnection, NoOpSessionContext.INSTANCE, callback)
    }

    def 'should execute update command protocol asynchronously'() {
        given:
        def updates = asList(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.updateCommandAsync(namespace, true, ACKNOWLEDGED, updates, callback)

        then:
        1 * executor.executeAsync({ compare(new UpdateCommandProtocol(namespace, true, ACKNOWLEDGED, null, updates), it) },
                                  internalConnection, NoOpSessionContext.INSTANCE, callback)
    }

    def 'should execute delete command protocol asynchronously'() {
        given:
        def deletes = asList(new DeleteRequest(new BsonDocument()))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.deleteCommandAsync(namespace, true, ACKNOWLEDGED, deletes, callback)

        then:
        1 * executor.executeAsync({ compare(new DeleteCommandProtocol(namespace, true, ACKNOWLEDGED, deletes), it) },
                                  internalConnection, NoOpSessionContext.INSTANCE, callback)
    }

    def 'should execute command protocol asynchronously'() {
        given:
        def command = new BsonDocument('ismaster', new BsonInt32(1))
        def validator = new NoOpFieldNameValidator()
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.commandAsync('test', command, slaveOk, validator, codec, callback)

        then:
        1 * executor.executeAsync({
            compare(new SimpleCommandProtocol('test', command, validator, codec).readPreference(expectedReadPreference), it)
        }, internalConnection, NoOpSessionContext.INSTANCE, callback)

        where:
        slaveOk  | expectedReadPreference
        true     | ReadPreference.secondaryPreferred()
        false    | ReadPreference.primary()
    }

    def 'should set slaveOk when executing command protocol on connection in SINGLE connection mode asynchronously'() {
        given:
        def command = new BsonDocument('ismaster', new BsonInt32(1))
        def validator = new NoOpFieldNameValidator()
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.SINGLE)
        internalConnection.description >> connectionDescription

        when:
        connection.commandAsync('test', command, false, validator, codec, callback)

        then:
        1 * executor.executeAsync({
            compare(new SimpleCommandProtocol('test', command, validator, codec).readPreference(expectedReadPreference), it)
        }, internalConnection, NoOpSessionContext.INSTANCE, callback)

        where:
        connectionDescription           | expectedSlaveOk  | expectedReadPreference
        standaloneConnectionDescription | true             | ReadPreference.secondaryPreferred()
        mongosConnectionDescription     | false            | ReadPreference.primary()
    }

    def 'should execute query protocol asynchronously'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.queryAsync(namespace, query, fields, 1, 2, slaveOk, false, true, false, true, false, decoder, callback)

        then:
        1 * executor.executeAsync({
                                      compare(new QueryProtocol(namespace, 2, 1, query, fields, decoder)
                                               .slaveOk(slaveOk)
                                               .tailableCursor(false)
                                               .awaitData(true)
                                               .noCursorTimeout(false)
                                               .partial(true)
                                               .oplogReplay(false)
                                       , it)
                             }, internalConnection, callback)

        where:
        slaveOk << [true, false]
    }

    def 'should set slaveOk when executing query protocol on connection in SINGLE connection mode asynchronously'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.SINGLE)
        internalConnection.description >> connectionDescription

        when:
        connection.queryAsync(namespace, query, fields, 1, 2, false, false, true, false, true, false, decoder, callback)

        then:
        1 * executor.executeAsync({
                                      compare(new QueryProtocol(namespace, 2, 1, query, fields, decoder)
                                               .slaveOk(expectedSlaveOk)
                                               .tailableCursor(false)
                                               .awaitData(true)
                                               .noCursorTimeout(false)
                                               .partial(true)
                                               .oplogReplay(false)
                                       , it)
                             }, internalConnection, callback)

        where:
        connectionDescription           | expectedSlaveOk
        standaloneConnectionDescription | true
        mongosConnectionDescription     | false
    }

    def 'should execute getmore protocol asynchronously'() {
        given:
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.getMoreAsync(namespace, 1000L, 1, codec, callback)

        then:
        1 * executor.executeAsync({ compare(new GetMoreProtocol(namespace, 1000L, 1, codec), it) }, internalConnection, callback)
    }

    def 'should execute kill cursor protocol asynchronously'() {
        given:
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.killCursorAsync([5], callback)

        then:
        1 * executor.executeAsync({ compare(new KillCursorProtocol(null, [5]), it) }, internalConnection, callback)

        when:
        connection.killCursorAsync(namespace, [5], callback)

        then:
        1 * executor.executeAsync({ compare(new KillCursorProtocol(namespace, [5]), it) }, internalConnection, callback)
    }
}

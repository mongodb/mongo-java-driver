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

package com.mongodb.internal.connection

import com.mongodb.MongoNamespace
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcernResult
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.diagnostics.logging.Logger
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.bulk.WriteRequest
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared
import spock.lang.Specification

import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.CustomMatchers.compare
import static com.mongodb.connection.ServerType.SHARD_ROUTER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER

class DefaultServerConnectionSpecification extends Specification {
    def namespace = new MongoNamespace('test', 'test')
    def internalConnection = Mock(InternalConnection)
    def callback = errorHandlingCallback(Mock(SingleResultCallback), Mock(Logger))
    @Shared
    def standaloneConnectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            3, STANDALONE, 100, 100, 100, [])
    @Shared
    def mongosConnectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
            3, SHARD_ROUTER, 100, 100, 100, [])

    def 'should execute insert protocol'() {
        given:
        def insertRequest = new InsertRequest(new BsonDocument())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new InsertProtocol(namespace, true, insertRequest), it) }, internalConnection) >> {
                WriteConcernResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.insert(namespace, true, insertRequest)

        then:
        result == WriteConcernResult.unacknowledged()
    }

    def 'should execute update protocol'() {
        given:
        def updateRequest = new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE)
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new UpdateProtocol(namespace, true, updateRequest), it) }, internalConnection) >> {
                WriteConcernResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.update(namespace, true,  updateRequest)

        then:
        result == WriteConcernResult.unacknowledged()
    }

    def 'should execute delete protocol'() {
        given:
        def deleteRequest = new DeleteRequest(new BsonDocument())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({ compare(new DeleteProtocol(namespace, true, deleteRequest), it) }, internalConnection) >> {
                WriteConcernResult.unacknowledged()
            }
        }
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        def result = connection.delete(namespace, true, deleteRequest)

        then:
        result == WriteConcernResult.unacknowledged()
    }

    def 'should execute query protocol'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def expectedResult = new QueryResult<>(namespace, [], 0, new ServerAddress())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new QueryProtocol(namespace, 2, 10, 5, query, fields, decoder)
                    .secondaryOk(secondaryOk)
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
        def result = connection.query(namespace, query, fields, 2, 10, 5, secondaryOk, false, true, false, true, false, decoder)

        then:
        result == expectedResult

        where:
        secondaryOk << [true, false]
    }

    def 'should set secondaryOk when executing query protocol on connection in SINGLE connection mode'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def expectedResult = new QueryResult<>(namespace, [], 0, new ServerAddress())
        def executor = Mock(ProtocolExecutor) {
            1 * execute({
                compare(new QueryProtocol(namespace, 2, 10, 5, query, fields, decoder)
                    .secondaryOk(expectedSecondaryOk)
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
        def result = connection.query(namespace, query, fields, 2, 10, 5, false, false, true, false, true, false, decoder)

        then:
        result == expectedResult

        where:
        connectionDescription           | expectedSecondaryOk
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
        connection.killCursor(namespace, [5])

        then:
        1 * executor.execute({ compare(new KillCursorProtocol(namespace, [5]), it) }, internalConnection)
    }

    def 'should execute insert protocol asynchronously'() {
        given:
        def insertRequest = new InsertRequest(new BsonDocument())
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.insertAsync(namespace, true, insertRequest, callback)

        then:
        1 * executor.executeAsync({ compare(new InsertProtocol(namespace, true, insertRequest), it) }, internalConnection,
                callback)
    }

    def 'should execute update protocol asynchronously'() {
        given:
        def updateRequest = new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE)
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.updateAsync(namespace, true, updateRequest, callback)

        then:
        1 * executor.executeAsync({ compare(new UpdateProtocol(namespace, true, updateRequest), it) },
                                  internalConnection, callback)
    }

    def 'should execute delete protocol asynchronously'() {
        given:
        def deleteRequest = new DeleteRequest(new BsonDocument())
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.deleteAsync(namespace, true, deleteRequest, callback)

        then:
        1 * executor.executeAsync({ compare(new DeleteProtocol(namespace, true, deleteRequest), it) }, internalConnection,
                callback)
    }

    def 'should execute command protocol asynchronously'() {
        given:
        def command = new BsonDocument(LEGACY_HELLO_LOWER, new BsonInt32(1))
        def validator = new NoOpFieldNameValidator()
        def codec = new BsonDocumentCodec()
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.commandAsync('test', command, validator, ReadPreference.primary(), codec, NoOpSessionContext.INSTANCE,
                getServerApi(), callback)

        then:
        1 * executor.executeAsync({
            compare(new CommandProtocolImpl('test', command, validator, ReadPreference.primary(), codec, getServerApi()), it)
        }, internalConnection, NoOpSessionContext.INSTANCE, callback)
    }

    def 'should execute query protocol asynchronously'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.MULTIPLE)

        when:
        connection.queryAsync(namespace, query, fields, 2, 10, 5, secondaryOk, false, true, false, true, false, decoder, callback)

        then:
        1 * executor.executeAsync({
                                      compare(new QueryProtocol(namespace, 2, 10, 5, query, fields, decoder)
                                               .secondaryOk(secondaryOk)
                                               .tailableCursor(false)
                                               .awaitData(true)
                                               .noCursorTimeout(false)
                                               .partial(true)
                                               .oplogReplay(false)
                                       , it)
                             }, internalConnection, callback)

        where:
        secondaryOk << [true, false]
    }

    def 'should set secondaryOk when executing query protocol on connection in SINGLE connection mode asynchronously'() {
        given:
        def decoder = new BsonDocumentCodec()
        def query = new BsonDocument('x', BsonBoolean.TRUE)
        def fields = new BsonDocument('y', new BsonInt32(1))
        def executor = Mock(ProtocolExecutor)
        def connection = new DefaultServerConnection(internalConnection, executor, ClusterConnectionMode.SINGLE)
        internalConnection.description >> connectionDescription

        when:
        connection.queryAsync(namespace, query, fields, 2, 10, 5, false, false, true, false, true, false, decoder, callback)

        then:
        1 * executor.executeAsync({
                                      compare(new QueryProtocol(namespace, 2, 10, 5, query, fields, decoder)
                                               .secondaryOk(expectedSecondaryOk)
                                               .tailableCursor(false)
                                               .awaitData(true)
                                               .noCursorTimeout(false)
                                               .partial(true)
                                               .oplogReplay(false)
                                       , it)
                             }, internalConnection, callback)

        where:
        connectionDescription           | expectedSecondaryOk
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
        connection.killCursorAsync(namespace, [5], callback)

        then:
        1 * executor.executeAsync({ compare(new KillCursorProtocol(namespace, [5]), it) }, internalConnection, callback)
    }
}

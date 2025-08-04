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

import com.mongodb.MongoClientSettings
import com.mongodb.MongoCommandException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.TimeoutSettings
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.OperationContext
import com.mongodb.internal.tracing.TracingManager
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static OperationUnitSpecification.getMaxWireVersionForServerVersion
import static com.mongodb.ReadPreference.primary
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION

class AsyncCommandBatchCursorSpecification extends Specification {

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def initialConnection = referenceCountedAsyncConnection()
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSource(connection)
        def timeoutContext = connectionSource.getOperationContext().getTimeoutContext()
        def firstBatch = createCommandResult([])
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(CURSOR_ID))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }

        def reply =  getMoreResponse([], 0)

        when:
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, batchSize, maxTimeMS, CODEC,
                null, connectionSource, initialConnection)
        then:
        1 * timeoutContext.setMaxTimeOverride(*_)

        when:
        def batch = nextBatch(cursor)

        then:
        1 * connection.commandAsync(NAMESPACE.getDatabaseName(), expectedCommand, *_) >> {
            it.last().onResult(reply, null)
        }
        batch.isEmpty()

        then:
        cursor.isClosed()

        then:
        cursor.close()

        then:
        connection.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        batchSize | maxTimeMS | expectedMaxTimeFieldValue
        0         | 0         | null
        2         | 0         | null
        0         | 100       | 100
    }

    def 'should close the cursor'() {
        given:
        def initialConnection = referenceCountedAsyncConnection()
        def serverVersion = new ServerVersion([3, 6, 0])
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)

        when:
        cursor.close()

        then:
        if (cursor.getServerCursor() != null) {
            1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(cursor.getServerCursor()), _, primary(), *_) >> {
                it.last().onResult(null, null)
            }
        }

        then:
        connection.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        firstBatch << [createCommandResult(), createCommandResult(FIRST_BATCH, 0)]
    }

    def 'should return the expected results from next'() {
        given:
        def initialConnection = referenceCountedAsyncConnection()
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSource(connection)

        when:
        def firstBatch = createCommandResult(FIRST_BATCH, 0)
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)

        then:
        nextBatch(cursor) == FIRST_BATCH

        then:
        connectionSource.getCount() == 0

        then:
        cursor.isClosed()

        when:
        nextBatch(cursor)

        then:
        def exception = thrown(IllegalStateException)
        exception.getMessage() == MESSAGE_IF_CLOSED_AS_CURSOR
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0
    }

    def 'should handle getMore when there are empty results but there is a cursor'() {
        given:
        def initialConnection = referenceCountedAsyncConnection()
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSource(connection)

        when:
        def firstBatch = createCommandResult([], CURSOR_ID)
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)
        def batch = nextBatch(cursor)

        then:
        1 * connection.commandAsync(*_) >> {
            connection.getCount() == 1
            connectionSource.getCount() == 1
            it.last().onResult(response, null)
        }

        1 * connection.commandAsync(*_) >> {
            connection.getCount() == 1
            connectionSource.getCount() == 1
            it.last().onResult(response2, null)
        }

        then:
        batch == SECOND_BATCH

        when:
        cursor.close()

        then:
        0 * connection._
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        serverVersion                | response              | response2
        new ServerVersion([3, 6, 0]) | getMoreResponse([])  | getMoreResponse(SECOND_BATCH, 0)
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore returns a response'() {
        given:
        def serverVersion =  new ServerVersion([3, 6, 0])
        def initialConnection = referenceCountedAsyncConnection(serverVersion, 'connectionOri', serverType)
        def connectionA = referenceCountedAsyncConnection(serverVersion, 'connectionA', serverType)
        def connectionB = referenceCountedAsyncConnection(serverVersion, 'connectionB', serverType)
        def connectionSource = getAsyncConnectionSource(serverType, connectionA, connectionB)

        def firstConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionA
        def secondConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionB
        def firstBatch = createCommandResult()

        when:
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        // simulate the user calling `close` while `getMore` is in flight
        // in LB mode the same connection is used to execute both `getMore` and `killCursors`
        1 * firstConnection.commandAsync(*_) >> {
            // `getMore` command
            cursor.close()
            ((SingleResultCallback<?>) it.last()).onResult(getMoreResponse([], responseCursorId), null)
        }

        then:
        if (responseCursorId > 0) {
            1 * secondConnection.commandAsync(*_) >> {
                // `killCursors` command
                ((SingleResultCallback<?>) it.last()).onResult(null, null)
            }
        }

        then:
        noExceptionThrown()

        then:
        connectionA.getCount() == 0
        connectionB.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0
        cursor.isClosed()

        where:
        serverType               | responseCursorId
        ServerType.LOAD_BALANCER | 42
        ServerType.LOAD_BALANCER | 0
        ServerType.STANDALONE    | 42
        ServerType.STANDALONE    | 0
    }

    def 'should throw concurrent operation assertion error'() {
        given:
        def serverVersion =  new ServerVersion([3, 6, 0])
        def initialConnection = referenceCountedAsyncConnection(serverVersion, 'connectionOri')
        def connectionA = referenceCountedAsyncConnection(serverVersion, 'connectionA')
        def connectionB = referenceCountedAsyncConnection(serverVersion, 'connectionB')
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        when:
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, createCommandResult(FIRST_BATCH, 42), 0, 0, CODEC,
                null, connectionSource, initialConnection)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        // simulate the user calling `cursor.next()` while `getMore` is in flight
        1 * connectionA.commandAsync(*_) >> {
            // `getMore` command
            nextBatch(cursor)
        }

        then:
        def exception = thrown(AssertionError)
        exception.getMessage() == MESSAGE_IF_CONCURRENT_OPERATION
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        def serverVersion = new ServerVersion([4, 4, 0])
        def initialConnection = referenceCountedAsyncConnection(serverVersion, 'connectionOri', serverType)
        def connectionA = referenceCountedAsyncConnection(serverVersion, 'connectionA', serverType)
        def connectionB = referenceCountedAsyncConnection(serverVersion, 'connectionB', serverType)
        def connectionSource = getAsyncConnectionSource(serverType, connectionA, connectionB)

        def firstConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionA
        def secondConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionB

        def firstBatch = createCommandResult()

        when:
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        1 * firstConnection.commandAsync(*_) >> {
            // Simulate the user calling close while the getMore is throwing a MongoException
            cursor.close()
            ((SingleResultCallback<?>) it.last()).onResult(null, MONGO_EXCEPTION)
        }

        then:
        1 * secondConnection.commandAsync(*_) >> {
            // `killCursors` command
            ((SingleResultCallback<?>) it.last()).onResult(null, null)
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        initialConnection.getCount() == 0
        cursor.isClosed()

        where:
        serverType << [ServerType.LOAD_BALANCER, ServerType.STANDALONE]
    }

    def 'should handle errors when calling close'() {
        given:
        def initialConnection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSourceWithResult(ServerType.STANDALONE) { [null, MONGO_EXCEPTION] }
        def firstBatch = createCommandResult()
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)

        when:
        cursor.close()

        then:
        cursor.isClosed()
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0
    }


    def 'should handle errors when getting a connection for getMore'() {
        given:
        def initialConnection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSourceWithResult(ServerType.STANDALONE) { [null, MONGO_EXCEPTION] }

        when:
        def firstBatch = createCommandResult()
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)

        then:
        nextBatch(cursor)

        when:
        nextBatch(cursor)

        then:
        thrown(MongoException)

        then:
        initialConnection.getCount() == 0
        connectionSource.getCount() == 1
    }

    def 'should handle errors when calling getMore'() {
        given:
        def serverVersion = new ServerVersion([3, 6, 0])
        def initialConnection = referenceCountedAsyncConnection()
        def connectionA = referenceCountedAsyncConnection(serverVersion, 'connectionA')
        def connectionB = referenceCountedAsyncConnection(serverVersion, 'connectionB')
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        when:
        def firstBatch = createCommandResult()
        def cursor = new AsyncCommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, firstBatch, 0, 0, CODEC,
                null, connectionSource, initialConnection)

        then:
        connectionSource.getCount() == 1

        when:
        nextBatch(cursor)
        nextBatch(cursor)

        then:
        1 * connectionA.commandAsync(*_) >> {
            connectionA.getCount() == 1
            connectionSource.getCount() == 1
            it.last().onResult(null, exception)
        }
        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        cursor.close()

        then:
        1 * connectionB.commandAsync(*_) >> {
            connectionB.getCount() == 1
            connectionSource.getCount() == 1
            it.last().onResult(null, null)
        }

        then:
        connectionA.getCount() == 0
        connectionB.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        exception << [COMMAND_EXCEPTION, MONGO_EXCEPTION]
    }

    List<Document> nextBatch(AsyncCommandBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get()
    }

    private static final MongoNamespace NAMESPACE = new MongoNamespace('db', 'coll')
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()
    private static final CURSOR_ID = 42
    private static final FIRST_BATCH = [new Document('_id', 1), new Document('_id', 2)]
    private static final SECOND_BATCH = [new Document('_id', 3), new Document('_id', 4)]
    private static final CODEC = new DocumentCodec()
    private static final MONGO_EXCEPTION = new MongoException('error')
    private static final COMMAND_EXCEPTION = new MongoCommandException(BsonDocument.parse('{"ok": false, "errmsg": "error"}'),
            SERVER_ADDRESS)

    private static BsonDocument getMoreResponse(results, cursorId = CURSOR_ID) {
        createCommandResult(results, cursorId, "nextBatch")
    }

    private static BsonDocument createCommandResult(List<?> results = FIRST_BATCH, Long cursorId = CURSOR_ID,
            String fieldNameContainingBatch = "firstBatch") {
        new BsonDocument("ok", new BsonInt32(1))
                .append("cursor",
                        new BsonDocument("ns", new BsonString(NAMESPACE.fullName))
                                .append("id", new BsonInt64(cursorId))
                                .append(fieldNameContainingBatch, new BsonArrayWrapper(results)))
    }

    private static BsonDocument createKillCursorsDocument(ServerCursor serverCursor) {
        new BsonDocument('killCursors', new BsonString(NAMESPACE.getCollectionName()))
                .append('cursors', new BsonArray(Collections.singletonList(new BsonInt64(serverCursor.id))))
    }

    AsyncConnection referenceCountedAsyncConnection() {
        referenceCountedAsyncConnection(new ServerVersion([3, 6, 0]))
    }

    AsyncConnection referenceCountedAsyncConnection(ServerVersion serverVersion, String name = 'connection',
            ServerType serverType = ServerType.STANDALONE) {
        def released = false
        def counter = 0
        def mock = Mock(AsyncConnection, name: name) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion.getVersionList())
                getServerAddress() >> SERVER_ADDRESS
                getServerType() >> serverType
            }
        }
        mock.retain() >> {
            if (released) {
                throw new IllegalStateException('Tried to retain AsyncConnection when already released')
            } else {
                counter += 1
            }
            mock
        }
        mock.release() >> {
            counter -= 1
            if (counter == 0) {
                released = true
            } else if (counter < 0) {
                throw new IllegalStateException('Tried to release AsyncConnection below 0')
            }
            counter
        }
        mock.getCount() >> { counter }
        mock
    }

    AsyncConnectionSource getAsyncConnectionSource(AsyncConnection... connections) {
        getAsyncConnectionSource(ServerType.STANDALONE, connections)
    }

    AsyncConnectionSource getAsyncConnectionSource(ServerType serverType, AsyncConnection... connections) {
        def index = -1
        getAsyncConnectionSourceWithResult(serverType) { index += 1; [connections.toList().get(index).retain(), null] }
    }

    def getAsyncConnectionSourceWithResult(ServerType serverType, Closure<?> connectionCallbackResults) {
        def released = false
        int counter = 0
        def mock = Mock(AsyncConnectionSource)
        mock.getServerDescription() >> {
            ServerDescription.builder()
                    .address(new ServerAddress())
                    .type(serverType)
                    .state(ServerConnectionState.CONNECTED)
                    .build()
        }
        OperationContext operationContext = Mock(OperationContext)
        operationContext.getTracingManager() >> TracingManager.NO_OP
        def timeoutContext = Spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(3, TimeUnit.SECONDS).build())))
        operationContext.getTimeoutContext() >> timeoutContext
        mock.getOperationContext() >> operationContext
        mock.getConnection(_) >> {
            if (counter == 0) {
                throw new IllegalStateException('Tried to use released AsyncConnectionSource')
            }
            def (result, error) = connectionCallbackResults()
            it[0].onResult(result, error)
        }
        mock.retain() >> {
            if (released) {
                throw new IllegalStateException('Tried to retain AsyncConnectionSource when already released')
            } else {
                counter += 1
            }
            mock
        }
        mock.release() >> {
            counter -= 1
            if (counter == 0) {
                released = true
            } else if (counter < 0) {
                throw new IllegalStateException('Tried to release AsyncConnectionSource below 0')
            }
            counter
        }
        mock.getCount() >> { counter }
        mock
    }
}

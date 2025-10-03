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
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.TimeoutSettings
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.NoOpSessionContext
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

import static com.mongodb.ReadPreference.primary
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class CommandBatchCursorSpecification extends Specification {

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connection = referenceCountedConnection()
        def connectionSource = getConnectionSource(connection)
        def operationContext = getOperationContext()
        def timeoutContext = operationContext.getTimeoutContext()

        def firstBatch = createCommandResult([])
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(CURSOR_ID))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }

        def reply =  getMoreResponse([], 0)

        when:
        def commandCoreCursor = new CommandCursor<>(firstBatch, batchSize, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, maxTimeMS, operationContext, commandCoreCursor)

        then:
        1 * timeoutContext.withMaxTimeOverride(*_)

        when:
        cursor.hasNext()

        then:
        1 * connection.command(NAMESPACE.getDatabaseName(), expectedCommand, *_) >>  reply

        then:
        !commandCoreCursor.isClosed()

        when:
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
        def initialConnection = referenceCountedConnection()
        def serverVersion = new ServerVersion([3, 6, 0])
        def connection = referenceCountedConnection(serverVersion)
        def connectionSource = getConnectionSource(connection)
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)

        when:
        cursor.close()

        then:
        if (cursor.getServerCursor() != null) {
            1 * connection.command(NAMESPACE.databaseName, createKillCursorsDocument(cursor.getServerCursor()), _, primary(), *_)
        }

        then:
        connection.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        firstBatch << [createCommandResult(FIRST_BATCH, 42), createCommandResult(FIRST_BATCH, 0)]
    }

    def 'should return the expected results from next'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connection = referenceCountedConnection()
        def connectionSource = getConnectionSource(connection)

        when:
        def firstBatch = createCommandResult(FIRST_BATCH, 0)
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)

        then:
        cursor.next() == FIRST_BATCH

        then:
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        then:
        // Unlike the AsyncCommandBatchCursor - the cursor isn't automatically closed
        !commandCoreCursor.isClosed()
    }

    def 'should handle getMore when there are empty results but there is a cursor'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connectionA = referenceCountedConnection(serverVersion, 'connectionA')
        def connectionB = referenceCountedConnection(serverVersion, 'connectionB')
        def connectionSource = getConnectionSource(connectionA, connectionB)

        when:
        def firstBatch = createCommandResult([], CURSOR_ID)
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)
        def batch = cursor.next()

        then:
        1 * connectionA.command(*_) >> {
            connectionA.getCount() == 1
            connectionSource.getCount() == 1
            response
        }

        1 * connectionB.command(*_) >> {
            connectionB.getCount() == 1
            connectionSource.getCount() == 1
            response2
        }

        then:
        batch == SECOND_BATCH

        then:
        connectionA.getCount() == 0
        connectionB.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        when:
        cursor.close()

        then:
        0 * connectionA._
        0 * connectionB._
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        serverVersion                | response              | response2
        new ServerVersion([3, 6, 0]) | getMoreResponse([])   | getMoreResponse(SECOND_BATCH, 0)
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore returns a response'() {
        given:
        def serverVersion =  new ServerVersion([3, 6, 0])
        def initialConnection = referenceCountedConnection(serverVersion, 'connectionOri', serverType)
        def connectionA = referenceCountedConnection(serverVersion, 'connectionA', serverType)
        def connectionB = referenceCountedConnection(serverVersion, 'connectionB', serverType)
        def connectionSource = getConnectionSource(serverType, connectionA, connectionB)

        def firstConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionA
        def secondConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionB

        def firstBatch = createCommandResult()

        when:
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)
        List<Document> batch = cursor.next()

        then:
        batch == FIRST_BATCH

        when:
        cursor.next()

        then:
        // simulate the user calling `close` while `getMore` is in flight
        // in LB mode the same connection is used to execute both `getMore` and `killCursors`
        1 * firstConnection.command(*_) >> {
            // `getMore` command
            cursor.close()
            getMoreResponse([], responseCursorId)
        }

        then:
        if (responseCursorId > 0) {
            1 * secondConnection.command(*_) >> null
        }

        then:
        IllegalStateException e = thrown()
        e.getMessage() == MESSAGE_IF_CLOSED_AS_CURSOR

        then:
        connectionA.getCount() == 0
        connectionB.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0
        commandCoreCursor.isClosed()

        where:
        serverType               | responseCursorId
        ServerType.LOAD_BALANCER | 42
        ServerType.LOAD_BALANCER | 0
        ServerType.STANDALONE    | 42
        ServerType.STANDALONE    | 0
    }

    def 'should throw concurrent operation illegal state exception'() {
        given:
        def serverVersion =  new ServerVersion([3, 6, 0])
        def initialConnection = referenceCountedConnection(serverVersion, 'connectionOri')
        def connectionA = referenceCountedConnection(serverVersion, 'connectionA')
        def connectionB = referenceCountedConnection(serverVersion, 'connectionB')
        def connectionSource = getConnectionSource(connectionA, connectionB)

        when:
        def commandCoreCursor = new CommandCursor<>(createCommandResult(FIRST_BATCH, 42), 0, CODEC,
                null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)
        def batch = cursor.next()

        then:
        batch == FIRST_BATCH

        when:
        cursor.next()

        then:
        // simulate the user calling `cursor.next()` while `getMore` is in flight
        1 * connectionA.command(*_) >> {
            // `getMore` command
            cursor.next()
        }

        then:
        def exception = thrown(IllegalStateException)
        exception.getMessage() == MESSAGE_IF_CONCURRENT_OPERATION
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        def serverVersion = new ServerVersion([4, 4, 0])
        def initialConnection = referenceCountedConnection(serverVersion, 'connectionOri', serverType)
        def connectionA = referenceCountedConnection(serverVersion, 'connectionA', serverType)
        def connectionB = referenceCountedConnection(serverVersion, 'connectionB', serverType)
        def connectionSource = getConnectionSource(serverType, connectionA, connectionB)

        def firstConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionA
        def secondConnection = serverType == ServerType.LOAD_BALANCER ? initialConnection : connectionB

        def firstBatch = createCommandResult()

        when:
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)
        def batch = cursor.next()

        then:
        batch == FIRST_BATCH

        when:
        cursor.next()

        then:
        1 * firstConnection.command(*_) >> {
            // Simulate the user calling close while the getMore is throwing a MongoException
            cursor.close()
            throw MONGO_EXCEPTION
        }

        then:
        1 * secondConnection.command(*_) >> {
            // `killCursors` command
            null
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        commandCoreCursor.isClosed()

        where:
        serverType << [ServerType.LOAD_BALANCER, ServerType.STANDALONE]
    }

    def 'should handle errors when calling close'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connectionSource = getConnectionSourceWithResult(ServerType.STANDALONE) { throw MONGO_EXCEPTION }
        def firstBatch = createCommandResult()
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)

        when:
        cursor.close()

        then:
        commandCoreCursor.isClosed()
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0
    }


    def 'should handle errors when getting a connection for getMore'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connection = referenceCountedConnection()
        def connectionSource = getConnectionSourceWithResult(ServerType.STANDALONE) { throw MONGO_EXCEPTION }

        when:
        def firstBatch = createCommandResult()
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)

        then:
        cursor.next()

        when:
        cursor.hasNext()

        then:
        thrown(MongoException)

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 1
    }

    def 'should handle errors when calling getMore'() {
        given:
        def initialConnection = referenceCountedConnection()
        def serverVersion = new ServerVersion([3, 6, 0])
        def connectionA = referenceCountedConnection(serverVersion, 'connectionA')
        def connectionB = referenceCountedConnection(serverVersion, 'connectionB')
        def connectionSource = getConnectionSource(connectionA, connectionB)

        when:
        def firstBatch = createCommandResult()
        def commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 0, operationContext, commandCoreCursor)
        then:
        connectionSource.getCount() == 1

        when:
        cursor.next()
        cursor.next()

        then:
        1 * connectionA.command(*_) >> {
            connectionA.getCount() == 1
            connectionSource.getCount() == 1
            throw exception
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        cursor.close()

        then:
        1 * connectionB.command(*_) >> {
            connectionB.getCount() == 1
            connectionSource.getCount() == 1
            null
        }

        then:
        connectionA.getCount() == 0
        connectionB.getCount() == 0
        initialConnection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        exception << [COMMAND_EXCEPTION, MONGO_EXCEPTION]
    }

    def 'should handle exceptions when closing'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
            _ * command(*_) >> { throw new MongoSocketException('No MongoD', SERVER_ADDRESS) }
        }
        def connectionSource = Stub(ConnectionSource) {
            getServerApi() >> null
            getConnection(_) >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def initialResults = createCommandResult([])
        def commandCoreCursor = new CommandCursor<>(initialResults, 2, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 100, operationContext, commandCoreCursor)

        when:
        cursor.close()

        then:
        notThrown(MongoSocketException)

        when:
        cursor.close()

        then:
        notThrown(Exception)
    }

    def 'should handle exceptions when killing cursor and a connection can not be obtained'() {
        given:
        def initialConnection = referenceCountedConnection()
        def connectionSource = Stub(ConnectionSource) {
            getConnection(_) >> { throw new MongoSocketOpenException("can't open socket", SERVER_ADDRESS, new IOException()) }
            getServerApi() >> null
        }
        connectionSource.retain() >> connectionSource

        def initialResults = createCommandResult([])
        def commandCoreCursor = new CommandCursor<>(initialResults, 2, CODEC, null, connectionSource, initialConnection)
        def cursor = new CommandBatchCursor<Document>(TimeoutMode.CURSOR_LIFETIME, 100, operationContext, commandCoreCursor)

        when:
        cursor.close()

        then:
        notThrown(MongoSocketException)

        when:
        cursor.close()

        then:
        notThrown(Exception)
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

    Connection referenceCountedConnection() {
        referenceCountedConnection(new ServerVersion([3, 6, 0]))
    }

    Connection referenceCountedConnection(ServerVersion serverVersion, String name = 'connection',
            ServerType serverType = ServerType.STANDALONE) {
        def released = false
        def counter = 0
        def mock = Mock(Connection, name: name) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion.getVersionList())
                getServerType() >> serverType
            }
        }
        mock.retain() >> {
            if (released) {
                throw new IllegalStateException('Tried to retain Connection when already released')
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
                throw new IllegalStateException('Tried to release Connection below 0')
            }
            counter
        }
        mock.getCount() >> { counter }
        mock
    }

    ConnectionSource getConnectionSource(Connection... connections) {
        getConnectionSource(ServerType.STANDALONE, connections)
    }

    ConnectionSource getConnectionSource(ServerType serverType, Connection... connections) {
        def index = -1
        getConnectionSourceWithResult(serverType) { index += 1; connections.toList().get(index).retain() }
    }

    def getConnectionSourceWithResult(ServerType serverType, Closure<?> connectionCallbackResults) {
        def released = false
        int counter = 0
        def mock = Mock(ConnectionSource)
        mock.getServerDescription() >> {
            ServerDescription.builder()
                    .address(new ServerAddress())
                    .type(serverType)
                    .state(ServerConnectionState.CONNECTED)
                    .build()
        }
        mock.getConnection(_ as OperationContext) >> {
            if (counter == 0) {
                throw new IllegalStateException('Tried to use released ConnectionSource')
            }
            connectionCallbackResults()
        }
        mock.retain() >> {
            if (released) {
                throw new IllegalStateException('Tried to retain ConnectionSource when already released')
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
                throw new IllegalStateException('Tried to release ConnectionSource below 0')
            }
            counter
        }
        mock.getCount() >> { counter }
        mock
    }

    OperationContext getOperationContext() {
        def timeoutContext = Spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(3, TimeUnit.SECONDS).build())))
        Spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext, null))
    }

}

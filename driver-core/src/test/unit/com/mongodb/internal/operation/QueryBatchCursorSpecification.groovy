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

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.QueryResult
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class QueryBatchCursorSpecification extends Specification {
    private static final MongoNamespace NAMESPACE = new MongoNamespace('db', 'coll')
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { connection }
            getServerApi() >> null
        }
        connectionSource.retain() >> connectionSource

        def cursorId = 42

        def firstBatch = new QueryResult(NAMESPACE, [], cursorId, SERVER_ADDRESS)
        def cursor = new QueryBatchCursor<Document>(firstBatch, 0, batchSize, maxTimeMS, new BsonDocumentCodec(), connectionSource,
                                                    connection)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(cursorId))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue))
        }

        def reply = new BsonDocument('ok', new BsonInt32(1))
                .append('cursor',
                        new BsonDocument('id', new BsonInt64(0))
                                .append('ns', new BsonString(NAMESPACE.getFullName()))
                                .append('nextBatch', new BsonArrayWrapper([])))

        when:
        cursor.hasNext()

        then:
        1 * connection.command(NAMESPACE.getDatabaseName(), expectedCommand, _, _, _, _, null, _) >> {
            reply
        }
        1 * connection.release()

        where:
        batchSize  | maxTimeMS  | expectedMaxTimeFieldValue
        0          | 0          | null
        2          | 0          | null
        0          | 100        | 100
    }

    def 'should handle exceptions when closing'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
            _ * killCursor(*_) >> { throw new MongoSocketException('No MongoD', SERVER_ADDRESS) }
            _ * command(*_) >> { throw new MongoSocketException('No MongoD', SERVER_ADDRESS) }
        }
        def connectionSource = Stub(ConnectionSource) {
            getServerApi() >> null
            getConnection() >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def firstBatch = new QueryResult(NAMESPACE, [], 42, SERVER_ADDRESS)
        def cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, 100, new BsonDocumentCodec(), connectionSource, connection)

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
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { throw new MongoSocketOpenException("can't open socket", SERVER_ADDRESS, new IOException()) }
            getServerApi() >> null
        }
        connectionSource.retain() >> connectionSource

        def firstBatch = new QueryResult(NAMESPACE, [], 42, SERVER_ADDRESS)
        def cursor = new QueryBatchCursor<Document>(firstBatch, 0, 2, 100, new BsonDocumentCodec(), connectionSource, connection)

        when:
        cursor.close()

        then:
        notThrown(MongoSocketException)

        when:
        cursor.close()

        then:
        notThrown(Exception)
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore returns a response'() {
        given:
        Connection conn = mockConnection(serverVersion)
        ConnectionSource connSource
        if (serverType == ServerType.LOAD_BALANCER) {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType)
        } else {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType, conn, mockConnection(serverVersion))
        }
        List<Document> firstBatch = [new Document()]
        QueryResult<Document> initialResult = new QueryResult<>(NAMESPACE, firstBatch, 1, SERVER_ADDRESS)
        Object getMoreResponse = useCommand
                ? emptyGetMoreCommandResponse(NAMESPACE, getMoreResponseHasCursor ? 42 : 0)
                : emptyGetMoreQueryResponse(NAMESPACE, SERVER_ADDRESS, getMoreResponseHasCursor ? 42 : 0)

        when:
        QueryBatchCursor<Document> cursor = new QueryBatchCursor<>(initialResult, 0, 0, 0, new DocumentCodec(), connSource, conn)
        List<Document> batch = cursor.next()

        then:
        batch == firstBatch

        when:
        cursor.next()

        then:
        // simulate the user calling `close` while `getMore` is in flight
        if (useCommand) {
            // in LB mode the same connection is used to execute both `getMore` and `killCursors`
            int numberOfInvocations = serverType == ServerType.LOAD_BALANCER
                    ? getMoreResponseHasCursor ? 2 : 1
                    : 1
            numberOfInvocations * conn.command(*_) >> {
                // `getMore` command
                cursor.close()
                getMoreResponse
            } >> {
                // `killCursors` command
                null
            }
        } else {
            1 * conn.getMore(*_) >> {
                cursor.close()
                getMoreResponse
            }
        }

        then:
        IllegalStateException e = thrown()
        e.getMessage() == 'Cursor has been closed'

        then:
        conn.getCount() == 1
        connSource.getCount() == 1

        where:
        serverVersion                | useCommand | getMoreResponseHasCursor | serverType
        new ServerVersion([5, 0, 0]) | true       | true                     | ServerType.LOAD_BALANCER
        new ServerVersion([5, 0, 0]) | true       | false                    | ServerType.LOAD_BALANCER
        new ServerVersion([3, 2, 0]) | true       | true                     | ServerType.STANDALONE
        new ServerVersion([3, 2, 0]) | true       | false                    | ServerType.STANDALONE
        new ServerVersion([3, 0, 0]) | false      | true                     | ServerType.STANDALONE
        new ServerVersion([3, 0, 0]) | false      | false                    | ServerType.STANDALONE
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        Connection conn = mockConnection(serverVersion)
        ConnectionSource connSource
        if (serverType == ServerType.LOAD_BALANCER) {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType)
        } else {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType, conn, mockConnection(serverVersion))
        }
        List<Document> firstBatch = [new Document()]
        QueryResult<Document> initialResult = new QueryResult<>(NAMESPACE, firstBatch, 1, SERVER_ADDRESS)
        String exceptionMessage = 'test'

        when:
        QueryBatchCursor<Document> cursor = new QueryBatchCursor<>(initialResult, 0, 0, 0, new DocumentCodec(), connSource, conn)
        List<Document> batch = cursor.next()

        then:
        batch == firstBatch

        when:
        cursor.next()

        then:
        // simulate the user calling `close` while `getMore` is in flight
        if (useCommand) {
            // in LB mode the same connection is used to execute both `getMore` and `killCursors`
            int numberOfInvocations = serverType == ServerType.LOAD_BALANCER ? 2 : 1
            numberOfInvocations * conn.command(*_) >> {
                // `getMore` command
                cursor.close()
                throw new MongoException(exceptionMessage)
            } >> {
                // `killCursors` command
                null
            }
        } else {
            1 * conn.getMore(*_) >> {
                cursor.close()
                throw new MongoException(exceptionMessage)
            }
        }

        then:
        MongoException e = thrown()
        e.getMessage() == exceptionMessage

        then:
        conn.getCount() == 1
        connSource.getCount() == 1

        where:
        serverVersion                | useCommand | serverType
        new ServerVersion([5, 0, 0]) | true       | ServerType.LOAD_BALANCER
        new ServerVersion([3, 2, 0]) | true       | ServerType.STANDALONE
        new ServerVersion([3, 0, 0]) | false      | ServerType.STANDALONE
    }

    /**
     * Creates a {@link Connection} with {@link Connection#getCount()} returning 1.
     */
    private Connection mockConnection(ServerVersion serverVersion) {
        int refCounter = 1
        Connection mockConn = Mock(Connection) {
            getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion.getVersionList())
            }
        }
        mockConn.retain() >> {
            if (refCounter == 0) {
                throw new IllegalStateException('Tried to retain Connection when already released')
            } else {
                refCounter += 1
            }
            mockConn
        }
        mockConn.release() >> {
            refCounter -= 1
            if (refCounter < 0) {
                throw new IllegalStateException('Tried to release Connection below 0')
            }
        }
        mockConn.getCount() >> { refCounter }
        mockConn
    }

    private ConnectionSource mockConnectionSource(ServerAddress serverAddress, ServerType serverType, Connection... connections) {
        int connIdx = 0
        int refCounter = 1
        ConnectionSource mockConnectionSource = Mock(ConnectionSource)
        mockConnectionSource.getServerDescription() >> {
            ServerDescription.builder()
                    .address(serverAddress)
                    .type(serverType)
                    .state(ServerConnectionState.CONNECTED)
                    .build()
        }
        mockConnectionSource.retain() >> {
            if (refCounter == 0) {
                throw new IllegalStateException('Tried to retain ConnectionSource when already released')
            } else {
                refCounter += 1
            }
            mockConnectionSource
        }
        mockConnectionSource.release() >> {
            refCounter -= 1
            if (refCounter < 0) {
                throw new IllegalStateException('Tried to release ConnectionSource below 0')
            }
        }
        mockConnectionSource.getCount() >> { refCounter }
        mockConnectionSource.getConnection() >> {
            if (refCounter == 0) {
                throw new IllegalStateException('Tried to use released ConnectionSource')
            }
            Connection conn
            if (connIdx < connections.length) {
                conn = connections[connIdx]
            } else {
                throw new IllegalStateException('Requested more than maxConnections=' + maxConnections)
            }
            connIdx++
            conn.retain()
        }
        mockConnectionSource
    }

    private static BsonDocument emptyGetMoreCommandResponse(MongoNamespace namespace, long cursorId) {
        new BsonDocument('ok', new BsonInt32(1))
                .append('cursor', new BsonDocument('id', new BsonInt64(cursorId))
                        .append('ns', new BsonString(namespace.getFullName()))
                        .append('nextBatch', new BsonArrayWrapper([])))
    }

    private static <T> QueryResult<T> emptyGetMoreQueryResponse(MongoNamespace namespace, ServerAddress serverAddress, long cursorId) {
        new QueryResult(namespace, [], cursorId, serverAddress)
    }
}

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

import com.mongodb.MongoCommandException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.QueryResult
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static OperationUnitSpecification.getMaxWireVersionForServerVersion
import static com.mongodb.ReadPreference.primary

class AsyncQueryBatchCursorSpecification extends Specification {

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSource(connection)

        def firstBatch = new QueryResult(NAMESPACE, [], 42, SERVER_ADDRESS)
        def cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, batchSize, maxTimeMS, CODEC, connectionSource,
                connection)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(CURSOR_ID))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue))
        }

        def reply =  documentResponse([], 0)

        when:
        def batch = nextBatch(cursor)

        then:
        1 * connection.commandAsync(NAMESPACE.getDatabaseName(), expectedCommand, _, _, _, _, _, _) >> {
            it[7].onResult(reply, null)
        }
        batch == null

        then:
        !cursor.isClosed()

        then:
        cursor.close()

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        batchSize | maxTimeMS | expectedMaxTimeFieldValue
        0         | 0         | null
        2         | 0         | null
        0         | 100       | 100
    }

    def 'should close the cursor'() {
        given:
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 0, 0, CODEC, connectionSource, connection)
        cursor.close()

        then:
        if (firstBatch.getCursor() != null) {
            if (serverVersion.compareTo(new ServerVersion(3, 2)) >= 0) {
                1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(firstBatch.cursor), _, primary(),
                        _, _, _, _) >> {
                    it[7].onResult(null, null)
                }
            } else {
                1 * connection.killCursorAsync(NAMESPACE, [42], _) >> {
                    it[2].onResult(null, null)
                }
            }
        }

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        serverVersion                | firstBatch
        new ServerVersion([3, 2, 0]) |  queryResult()
        new ServerVersion([3, 2, 0]) |  queryResult(FIRST_BATCH, 0)
        new ServerVersion([3, 0, 0]) |  queryResult()
        new ServerVersion([3, 0, 0]) |  queryResult(FIRST_BATCH, 0)
    }

    def 'should return the expected results from next'() {
        given:
        def connectionSource = getAsyncConnectionSource(referenceCountedAsyncConnection())

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(FIRST_BATCH, 0), 0, 0, 0, CODEC, connectionSource, null)

        then:
        nextBatch(cursor) == FIRST_BATCH

        then:
        connectionSource.getCount() == 0

        then:
        cursor.isClosed()

        when:
        nextBatch(cursor)

        then:
        def exception = thrown(MongoException)
        exception.getMessage() == 'next() called after the cursor was closed.'

        when:
        tryNextBatch(cursor)

        then:
        exception = thrown(MongoException)
        exception.getMessage() == 'tryNext() called after the cursor was closed.'
    }

    def 'should return the expected results from tryNext'() {
        given:
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        def firstBatch = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def secondBatch = []
        def thirdBatch = [new Document('_id', 4), new Document('_id', 5)]

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(firstBatch), 6, 2, 0, CODEC, connectionSource, connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == firstBatch

        when:
        batch = tryNextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connectionA.commandAsync(_, _, _, _, _, _, _, _) >> { it[7].onResult(documentResponse(secondBatch), null) }
        } else {
            1 * connectionA.getMoreAsync(_, _, _, _, _) >> { it[4].onResult(queryResult(secondBatch), null) }
        }

        then:
        batch == null
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        batch = tryNextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connectionB.commandAsync(_, _, _, _, _, _, _, _) >> {
                connectionB.getCount() == 1
                connectionSource.getCount() == 1
                it[7].onResult(documentResponse(thirdBatch, 0), null)
            }
        } else {
            1 * connectionB.getMoreAsync(_, _, _, _, _) >> {
                connectionB.getCount() == 1
                connectionSource.getCount() == 1
                it[4].onResult(queryResult(thirdBatch, 0), null)
            }
        }
        0 * connectionB.killCursorAsync(_, _, _) >> { it[2].onResult(null, null) }

        then:
        batch == thirdBatch
        connectionB.getCount() == 0
        connectionSource.getCount() == 0

        when:
        batch = tryNextBatch(cursor)

        then:
        batch == null
        connectionSource.getCount() == 0

        where:
        serverVersion                | commandAsync
        new ServerVersion([3, 2, 0]) | true
        new ServerVersion([3, 0, 0]) | false
    }

    def 'should respect the limit'() {
        given:
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        def firstBatch = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def secondBatch = [new Document('_id', 4), new Document('_id', 5)]
        def thirdBatch = [new Document('_id', 6)]

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(firstBatch), 6, 2, 0, CODEC, connectionSource,
                connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == firstBatch

        when:
        batch = nextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connectionA.commandAsync(_, _, _, _, _, _, _, _) >> { it[7].onResult(documentResponse(secondBatch), null) }
        } else {
            1 * connectionA.getMoreAsync(_, _, _, _, _) >> { it[4].onResult(queryResult(secondBatch), null) }
        }

        then:
        batch == secondBatch
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        batch = nextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connectionB.commandAsync(_, _, _, _, _, _, _, _) >> {
                connectionB.getCount() == 1
                connectionSource.getCount() == 1
                it[7].onResult(documentResponse(thirdBatch, 0), null)
            }
        } else {
            1 * connectionB.getMoreAsync(_, _, _, _, _) >> {
                connectionB.getCount() == 1
                connectionSource.getCount() == 1
                it[4].onResult(queryResult(thirdBatch, 0), null)
            }
        }
        0 * connectionB.killCursorAsync(_, _, _) >> { it[2].onResult(null, null) }

        then:
        batch == thirdBatch
        connectionB.getCount() == 0
        connectionSource.getCount() == 0

        when:
        batch = nextBatch(cursor)

        then:
        batch == null
        connectionSource.getCount() == 0

        where:
        serverVersion                | commandAsync
        new ServerVersion([3, 2, 0]) | true
        new ServerVersion([3, 0, 0]) | false
    }


    def 'should close the cursor immediately if the limit has been reached'() {
        given:
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)
        def queryResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult, 1, 0, 0, CODEC, connectionSource, connection)

        then:
        if (serverVersion.compareTo(new ServerVersion(3, 2)) >= 0) {
            1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(queryResult.cursor), _, primary(),
                    _, _, _, _) >> {
                it[7].onResult(null, null)
            }
        } else {
            1 * connection.killCursorAsync(NAMESPACE, [42], _) >> {
                it[2].onResult(null, null)
            }
        }

        when:
        cursor.close()

        then:
        0 * connection.killCursorAsync(_, _, _)

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        serverVersion << [new ServerVersion([3, 2, 0]), new ServerVersion([3, 0, 0])]
    }

    def 'should handle getMore when there are empty results but there is a cursor'() {
        given:
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult([], 42), 3, 0, 0, CODEC, connectionSource, connection)
        def batch = nextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connection.commandAsync(_, _, _, _, _, _, _, _) >> {
                connection.getCount() == 1
                connectionSource.getCount() == 1
                it[7].onResult(response, null)
            }

            1 * connection.commandAsync(_, _, _, _, _, _, _, _) >> {
                connection.getCount() == 1
                connectionSource.getCount() == 1
                it[7].onResult(response2, null)
            }
        } else {
            1 * connection.getMoreAsync(_, _, _, _, _) >> {
                connection.getCount() == 1
                connectionSource.getCount() == 1
                it[4].onResult(response, null)
            }

            1 * connection.getMoreAsync(_, _, _, _, _) >> {
                connection.getCount() == 1
                connectionSource.getCount() == 1
                it[4].onResult(response2, null)
            }
        }
        0 * connection.killCursorAsync(_, _, _)

        then:
        batch == SECOND_BATCH

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        when:
        cursor.close()

        then:
        0 * connection._
        connectionSource.getCount() == 0

        where:
        serverVersion                | commandAsync | response              | response2
        new ServerVersion([3, 2, 0]) | true         | documentResponse([])  | documentResponse(SECOND_BATCH, 0)
        new ServerVersion([3, 0, 0]) | false        | queryResult([])       | queryResult(SECOND_BATCH, 0)
    }

    def 'should kill the cursor in the getMore if limit is reached'() {
        given:
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)
        def initialResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(initialResult, 3, 0, 0, CODEC, connectionSource, connection)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        batch = nextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connection.commandAsync(_, _, _, _, _, _, _, _) >> {
                it[7].onResult(response, null)
            }
            1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(initialResult.cursor), _, primary(), _, _,
                    null, _) >> {
                it[7].onResult(null, null)
            }
        } else {
            1 * connection.getMoreAsync(_, _, _, _, _) >> {
                it[4].onResult(response, null)
            }
            1 * connection.killCursorAsync(NAMESPACE, [initialResult.cursor.id], _) >> { it[2].onResult(null, null) }
        }

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        when:
        cursor.close()

        then:
        0 * connection.killCursorAsync(_, _, _)
        0 * connection.commandAsync(_, _, _, _, _)
        connectionSource.getCount() == 0

        where:
        serverVersion                | commandAsync         | response
        new ServerVersion([3, 2, 0]) | true                 | documentResponse(SECOND_BATCH)
        new ServerVersion([3, 0, 0]) | false                | queryResult(SECOND_BATCH)
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore returns a response'() {
        given:
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)
        def initialResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(initialResult, 0, 0, 0, CODEC, connectionSource, connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        if (commandAsync) {
            _ * connectionA.commandAsync(_, _, _, _, _, _, _, _) >> {
                // Simulate the user calling close while the getMore is in flight
                cursor.close()
                it[7].onResult(response, null)
            } >> {
                it[7].onResult(response2, null)
            }
        } else {
            _ * connectionA.getMoreAsync(_, _, _, _, _) >> {
                // Simulate the user calling close while the getMore is in flight
                cursor.close()
                it[4].onResult(response, null)
            } >> {
                it[4].onResult(response2, null)
            }
        }

        then:
        noExceptionThrown()

        then:
        connectionA.getCount() == 0
        if (response2 == null) { //otherwise connectionSource is released asynchronously, which is not easy to verify
            connectionSource.getCount() == 0
        }
        cursor.isClosed()

        where:
        serverVersion                | commandAsync  | response                | response2
        new ServerVersion([3, 2, 0]) | true          | documentResponse([])    | documentResponse([], 0)
        new ServerVersion([3, 2, 0]) | true          | documentResponse([], 0) | null
        new ServerVersion([3, 0, 0]) | false         | new QueryResult(NAMESPACE, [], 42, SERVER_ADDRESS) |
                new QueryResult(NAMESPACE, [], 0, SERVER_ADDRESS)
        new ServerVersion([3, 0, 0]) | false         | new QueryResult(NAMESPACE, [], 0, SERVER_ADDRESS) | null
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)
        def initialResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(initialResult, 0, 0, 0, CODEC, connectionSource, connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connectionA.commandAsync(_, _, _, _, _, _, _, _) >> {
                // Simulate the user calling close while the getMore is throwing a MongoException
                cursor.close()
                it[7].onResult(null, MONGO_EXCEPTION)
            }
        } else {
            1 * connectionA.getMoreAsync(_, _, _, _, _) >> {
                // Simulate the user calling close while the getMore is throwing a MongoException
                cursor.close()
                it[4].onResult(null, MONGO_EXCEPTION)
            }
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        cursor.isClosed()

        where:
        serverVersion                | commandAsync
        new ServerVersion([3, 2, 0]) | true
        new ServerVersion([3, 0, 0]) | false
    }

    def 'should handle errors when calling close'() {
        given:
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSourceWithResult { [null, MONGO_EXCEPTION] }
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(), 0, 0, 0, CODEC, connectionSource, connection)

        when:
        cursor.close()
        nextBatch(cursor)

        then:
        def exception = thrown(MongoException)
        exception.getMessage() == 'next() called after the cursor was closed.'

        then:
        cursor.isClosed()
        connectionSource.getCount() == 0

        when:
        tryNextBatch(cursor)

        then:
        exception = thrown(MongoException)
        exception.getMessage() == 'tryNext() called after the cursor was closed.'
    }


    def 'should handle errors when getting a connection for getMore'() {
        given:
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSourceWithResult { [null, MONGO_EXCEPTION] }

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(), 0, 0, 0, CODEC, connectionSource, connection)

        then:
        nextBatch(cursor)

        when:
        nextBatch(cursor)

        then:
        thrown(MongoException)

        then:
        connectionSource.getCount() == 1

        when:
        cursor.close()

        then:
        connectionSource.getCount() == 0
    }

    def 'should handle errors when calling getMore'() {
        given:
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult([]), 0, 0, 0, CODEC, connectionSource,
                connectionA)

        then:
        connectionSource.getCount() == 1

        when:
        nextBatch(cursor)

        then:
        if (commandAsync) {
            1 * connectionA.commandAsync(_, _, _, _, _, _, _, _) >> {
                connectionA.getCount() == 1
                connectionSource.getCount() == 1
                it[7].onResult(null, exception)
            }
        } else {
            1 * connectionA.getMoreAsync(_, _, _, _, _) >> {
                connectionA.getCount() == 1
                connectionSource.getCount() == 1
                it[4].onResult(null, exception)
            }
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        cursor.close()

        then:
        connectionSource.getCount() == 1

        where:
        serverVersion                | commandAsync | exception
        new ServerVersion([3, 2, 0]) | true         | COMMAND_EXCEPTION
        new ServerVersion([3, 2, 0]) | true         | MONGO_EXCEPTION
        new ServerVersion([3, 0, 0]) | false        | COMMAND_EXCEPTION
        new ServerVersion([3, 0, 0]) | false        | MONGO_EXCEPTION
    }

    List<Document> nextBatch(AsyncQueryBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get()
    }

    List<Document> tryNextBatch(AsyncQueryBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.tryNext(futureResultCallback)
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

    def documentResponse(results, cursorId = 42) {
        new BsonDocument('ok', new BsonInt32(1)).append('cursor',
                new BsonDocument('id', new BsonInt64(cursorId)).append('ns',
                        new BsonString(NAMESPACE.getFullName()))
                        .append('nextBatch', new BsonArrayWrapper(results)))
    }

    def queryResult(results = FIRST_BATCH, cursorId = 42) {
        new QueryResult(NAMESPACE, results, cursorId, SERVER_ADDRESS)
    }

    def referenceCountedAsyncConnection() {
        referenceCountedAsyncConnection(new ServerVersion([3, 2, 0]))
    }

    def referenceCountedAsyncConnection(ServerVersion serverVersion) {
        def released = false
        def counter = 0
        def mock = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion.getVersionList())
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
        }
        mock.getCount() >> { counter }
        mock
    }

    def getAsyncConnectionSource(AsyncConnection... connections) {
        def index = -1
        getAsyncConnectionSourceWithResult { index += 1; [connections.toList().get(index).retain(), null] }
    }

    def getAsyncConnectionSourceWithResult(connectionCallbackResults) {
        def released = false
        int counter = 0
        def mock = Mock(AsyncConnectionSource)
        mock.getServerDescription() >> {
            ServerDescription.builder()
                    .address(new ServerAddress())
                    .type(ServerType.STANDALONE)
                    .state(ServerConnectionState.CONNECTED)
                    .build()
        }
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
        }
        mock.getCount() >> { counter }
        mock
    }

    BsonDocument createKillCursorsDocument(ServerCursor serverCursor) {
        new BsonDocument('killCursors', new BsonString(NAMESPACE.getCollectionName()))
                .append('cursors', new BsonArray(Collections.singletonList(new BsonInt64(serverCursor.id))))
    }

}

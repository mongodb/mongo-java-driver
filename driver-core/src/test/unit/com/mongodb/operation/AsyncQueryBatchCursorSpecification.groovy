/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.MongoNamespace
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.QueryResult
import com.mongodb.connection.ServerVersion
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.SECONDS

class AsyncQueryBatchCursorSpecification extends Specification {
    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        connectionSource.retain() >> connectionSource

        def database = 'test'
        def collection = 'AsyncQueryBatchCursorSpecification'
        def cursorId = 42

        def namespace = new MongoNamespace(database, collection)
        def firstBatch = new QueryResult(namespace, [], cursorId, new ServerAddress())
        def cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, batchSize, maxTimeMS, new DocumentCodec(), connectionSource,
                                                         connection)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(cursorId))
                .append('collection', new BsonString(collection))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue))
        }

        def reply = new BsonDocument('ok', new BsonInt32(1))
                .append('cursor',
                        new BsonDocument('id', new BsonInt64(0))
                                .append('ns', new BsonString(namespace.getFullName()))
                                .append('nextBatch', new BsonArrayWrapper([])))

        when:
        def batch = nextBatch(cursor)

        then:
        1 * connection.commandAsync(database, expectedCommand, _, _, _, _) >> {
            it[5].onResult(reply, null)
        }
        1 * connection.release()
        batch == null

        where:
        batchSize  | maxTimeMS  | expectedMaxTimeFieldValue
        0          | 0          | null
        2          | 0          | null
        0          | 100        | 100
    }

    List<Document> nextBatch(AsyncQueryBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get(60, SECONDS)
    }
}

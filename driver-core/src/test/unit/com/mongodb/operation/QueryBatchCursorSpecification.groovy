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
import com.mongodb.MongoSocketException
import com.mongodb.ServerAddress
import com.mongodb.binding.ConnectionSource
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.QueryResult
import com.mongodb.connection.ServerVersion
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

class QueryBatchCursorSpecification extends Specification {
    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def database = 'test'
        def collection = 'QueryBatchCursorSpecification'
        def cursorId = 42

        def namespace = new MongoNamespace(database, collection)
        def firstBatch = new QueryResult(namespace, [], cursorId, new ServerAddress())
        def cursor = new QueryBatchCursor<Document>(firstBatch, 0, batchSize, maxTimeMS, new BsonDocumentCodec(), connectionSource,
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
        cursor.hasNext()

        then:
        1 * connection.command(database, expectedCommand, _, _, _, _) >> {
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
        def serverAddress = new ServerAddress()
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
            _ * killCursor(_, _) >> { throw new MongoSocketException('No MongoD', serverAddress) }
            _ * command(_, _, _, _, _) >> { throw new MongoSocketException('No MongoD', serverAddress) }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def namespace = new MongoNamespace('test', 'QueryBatchCursorSpecification')
        def firstBatch = new QueryResult(namespace, [], 42, serverAddress)
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
}

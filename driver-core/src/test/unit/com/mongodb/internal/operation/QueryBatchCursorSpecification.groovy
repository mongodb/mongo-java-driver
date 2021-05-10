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

import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.ConnectionDescription
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.QueryResult
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import static com.mongodb.ClusterFixture.DEFAULT_CSOT_FACTORY
import static com.mongodb.ClusterFixture.MAX_AWAIT_TIME_MS_CSOT_FACTORY
import static com.mongodb.ClusterFixture.NO_CSOT_FACTORY

class QueryBatchCursorSpecification extends Specification {
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

        def database = 'test'
        def collection = 'QueryBatchCursorSpecification'
        def cursorId = 42

        def namespace = new MongoNamespace(database, collection)
        def firstBatch = new QueryResult(namespace, [], cursorId, new ServerAddress())
        def cursor = new QueryBatchCursor<Document>(csotFactory.create(), firstBatch, 0, batchSize, new BsonDocumentCodec(),
                connectionSource, connection, null)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(cursorId))
                .append('collection', new BsonString(collection))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue as long))
        }

        def reply = new BsonDocument('ok', new BsonInt32(1))
                .append('cursor',
                        new BsonDocument('id', new BsonInt64(0))
                                .append('ns', new BsonString(namespace.getFullName()))
                                .append('nextBatch', new BsonArrayWrapper([])))

        when:
        cursor.hasNext()

        then:
        1 * connection.command(database, expectedCommand, _, _, _, _, null) >> {
            reply
        }
        1 * connection.release()

        where:
        batchSize  | csotFactory                      | expectedMaxTimeFieldValue
        0          | NO_CSOT_FACTORY                  | null
        2          | NO_CSOT_FACTORY                  | null
        0          | MAX_AWAIT_TIME_MS_CSOT_FACTORY   | 9999
    }

    def 'should handle exceptions when closing'() {
        given:
        def serverAddress = new ServerAddress()
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
            _ * killCursor(_, _) >> { throw new MongoSocketException('No MongoD', serverAddress) }
            _ * command(_, _, _, _, _) >> { throw new MongoSocketException('No MongoD', serverAddress) }
        }
        def connectionSource = Stub(ConnectionSource) {
            getServerApi() >> null
            getConnection() >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def namespace = new MongoNamespace('test', 'QueryBatchCursorSpecification')
        def firstBatch = new QueryResult(namespace, [], 42, serverAddress)
        def cursor = new QueryBatchCursor<Document>(DEFAULT_CSOT_FACTORY.create(), firstBatch, 0, 2, new BsonDocumentCodec(),
                connectionSource, connection)

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
        def serverAddress = new ServerAddress()
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { throw new MongoSocketOpenException("can't open socket", serverAddress, new IOException()) }
        }
        connectionSource.retain() >> connectionSource

        def namespace = new MongoNamespace('test', 'QueryBatchCursorSpecification')
        def firstBatch = new QueryResult(namespace, [], 42, serverAddress)
        def cursor = new QueryBatchCursor<Document>(DEFAULT_CSOT_FACTORY.create(), firstBatch, 0, 2, new BsonDocumentCodec(),
                connectionSource, connection)

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

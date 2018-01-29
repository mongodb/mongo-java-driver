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

package com.mongodb.connection

import com.mongodb.MongoQueryException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getCredentialList
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.connection.ProtocolTestHelper.execute
import static org.bson.BsonDocument.parse

class QueryProtocolCommandEventSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection;

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialList(), null, null, [], null)
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open();
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should deliver start and completed command events with numberToReturn'() {
        given:
        def documents = [new BsonDocument('_id', new BsonInt32(1)),
                         new BsonDocument('_id', new BsonInt32(2)),
                         new BsonDocument('_id', new BsonInt32(3)),
                         new BsonDocument('_id', new BsonInt32(4)),
                         new BsonDocument('_id', new BsonInt32(5))]
        collectionHelper.insertDocuments(documents)

        def filter = parse('{_id : {$gt : 0}}')
        def projection = parse('{_id : 1}')
        def skip = 1
        def protocol = new QueryProtocol(getNamespace(), skip, 2, filter, projection, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def result = execute(protocol, connection, async)

        then:
        def response = new BsonDocument('cursor',
                                        new BsonDocument('id', new BsonInt64(result.cursor.id))
                                                .append('ns', new BsonString(getNamespace().getFullName()))
                                                .append('firstBatch', new BsonArray(documents.subList(1, 3))))
                .append('ok', new BsonDouble(1.0))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'find',
                                                                     new BsonDocument('find', new BsonString(getCollectionName()))
                                                                             .append('filter', filter)
                                                                             .append('projection', projection)
                                                                             .append('skip', new BsonInt32(skip))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'find', response, 0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events with limit and batchSize'() {
        given:
        def documents = [new BsonDocument('_id', new BsonInt32(1)),
                         new BsonDocument('_id', new BsonInt32(2)),
                         new BsonDocument('_id', new BsonInt32(3)),
                         new BsonDocument('_id', new BsonInt32(4)),
                         new BsonDocument('_id', new BsonInt32(5))]
        collectionHelper.insertDocuments(documents)

        def filter = parse('{_id : {$gt : 0}}')
        def projection = parse('{_id : 1}')
        def skip = 1
        def limit = 1000
        def batchSize = 2
        def protocol = new QueryProtocol(getNamespace(), skip, limit, batchSize, filter, projection, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def result = execute(protocol, connection, async)

        then:
        def response = new BsonDocument('cursor',
                                        new BsonDocument('id', new BsonInt64(result.cursor.id))
                                                .append('ns', new BsonString(getNamespace().getFullName()))
                                                .append('firstBatch', new BsonArray(documents.subList(1, 3))))
                .append('ok', new BsonDouble(1.0))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'find',
                                                                     new BsonDocument('find', new BsonString(getCollectionName()))
                                                                             .append('filter', filter)
                                                                             .append('projection', projection)
                                                                             .append('skip', new BsonInt32(skip))
                                                                             .append('limit', new BsonInt32(limit))
                                                                             .append('batchSize', new BsonInt32(batchSize))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'find', response, 0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events when there is no projection'() {
        given:
        def filter = parse('{_id : {$gt : 0}}')
        def projection = null
        def skip = 0
        def limit = 0
        def batchSize = 0
        def protocol = new QueryProtocol(getNamespace(), skip, limit, batchSize, filter, projection, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def queryResult = execute(protocol, connection, async)

        then:
        def response = new BsonDocument('cursor',
                                        new BsonDocument('id', new BsonInt64(queryResult.cursor ? queryResult.cursor.id : 0L))
                                                .append('ns', new BsonString(getNamespace().getFullName()))
                                                .append('firstBatch', new BsonArray()))
                .append('ok', new BsonDouble(1.0))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'find',
                                                                     new BsonDocument('find', new BsonString(getCollectionName()))
                                                                             .append('filter', filter)),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'find', response, 0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events when there are boolean options'() {
        given:
        collectionHelper.create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(10000))
        def filter = new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(0, 0)))
        def projection = null
        def skip = 0
        def limit = 0
        def batchSize = 0
        def protocol = new QueryProtocol(getNamespace(), skip, limit, batchSize, filter, projection, new BsonDocumentCodec())
        protocol.partial = true
        protocol.noCursorTimeout = true
        protocol.tailableCursor = true
        protocol.awaitData = true
        protocol.partial = true
        protocol.oplogReplay = true

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def queryResult = execute(protocol, connection, async)

        then:
        def response = new BsonDocument('cursor',
                                        new BsonDocument('id', new BsonInt64(queryResult.cursor ? queryResult.cursor.id : 0L))
                                                .append('ns', new BsonString(getNamespace().getFullName()))
                                                .append('firstBatch', new BsonArray()))
                .append('ok', new BsonDouble(1.0))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'find',
                                                                     new BsonDocument('find', new BsonString(getCollectionName()))
                                                                             .append('filter', filter)
                                                                             .append('tailable', BsonBoolean.TRUE)
                                                                             .append('oplogReplay', BsonBoolean.TRUE)
                                                                             .append('noCursorTimeout', BsonBoolean.TRUE)
                                                                             .append('awaitData', BsonBoolean.TRUE)
                                                                             .append('allowPartialResults', BsonBoolean.TRUE)),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'find', response, 0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events with meta operators'() {
        given:
        def documents = [new BsonDocument('_id', new BsonInt32(1)),
                         new BsonDocument('_id', new BsonInt32(2)),
                         new BsonDocument('_id', new BsonInt32(3)),
                         new BsonDocument('_id', new BsonInt32(4)),
                         new BsonDocument('_id', new BsonInt32(5))]
        collectionHelper.insertDocuments(documents)

        def filter = parse('{_id : {$gt : 0}}')
        def sort = parse('{_id : -1}')
        def comment = new BsonString('this is a comment')
        def hint = parse('{_id : 1}')
        def maxScan = new BsonInt32(5000)
        def maxTimeMS = new BsonInt64(6000)
        def min = parse('{_id : 0}')
        def max = parse('{_id : 6}')
        def returnKey = BsonBoolean.FALSE
        def showDiskLoc = BsonBoolean.FALSE
        def snapshot = BsonBoolean.FALSE
        def query = new BsonDocument().append('$query', filter)
                                      .append('$orderby', sort)
                                      .append('$comment', comment)
                                      .append('$hint', hint)
                                      .append('$maxTimeMS', maxTimeMS)
                                      .append('$maxScan', maxScan)
                                      .append('$min', min)
                                      .append('$max', max)
                                      .append('$returnKey', returnKey)
                                      .append('$showDiskLoc', showDiskLoc)
                                      .append('$snapshot', snapshot)
        def projection = parse('{_id : 1}')
        def skip = 1
        def limit = 1000
        def batchSize = 2
        def protocol = new QueryProtocol(getNamespace(), skip, limit, batchSize, query, projection, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def result = execute(protocol, connection, async)
        def cursorId = result.cursor ? new BsonInt64(result.cursor.id) : new BsonInt64(0)

        then:
        def response = new BsonDocument('cursor',
                                        new BsonDocument('id', cursorId)
                                                .append('ns', new BsonString(getNamespace().getFullName()))
                                                .append('firstBatch', new BsonArray([documents[3], documents[2]])))
                .append('ok', new BsonDouble(1.0))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'find',
                                                                     new BsonDocument('find', new BsonString(getCollectionName()))
                                                                             .append('filter', filter)
                                                                             .append('sort', sort)
                                                                             .append('comment', comment)
                                                                             .append('hint', hint)
                                                                             .append('maxTimeMS', maxTimeMS)
                                                                             .append('maxScan', maxScan)
                                                                             .append('min', min)
                                                                             .append('max', max)
                                                                             .append('returnKey', returnKey)
                                                                             .append('showRecordId', showDiskLoc)
                                                                             .append('snapshot', snapshot)
                                                                             .append('projection', projection)
                                                                             .append('skip', new BsonInt32(skip))
                                                                             .append('limit', new BsonInt32(limit))
                                                                             .append('batchSize', new BsonInt32(batchSize))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'find', response, 0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events for an explain command when there is a $explain meta operator'() {
        given:
        def filter = new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(0, 0)))
        def query = new BsonDocument('$query', filter).append('$explain', new BsonInt32(1))
        def projection = new BsonDocument('_id', new BsonInt32(1))
        def skip = 10
        def limit = -20
        def batchSize = 0
        def protocol = new QueryProtocol(getNamespace(), skip, limit, batchSize, query, projection, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def response = execute(protocol, connection, async)
        def expectedResponse = new BsonDocument('ok', new BsonDouble(1));
        expectedResponse.putAll(response.results[0]);

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'explain',
                                                                     new BsonDocument('explain',
                                                                                      new BsonDocument('find',
                                                                                                       new BsonString(getCollectionName()))
                                                                                              .append('filter', filter)
                                                                                              .append('skip', new BsonInt32(skip))
                                                                                              .append('limit', new BsonInt32(limit))
                                                                                              .append('projection', projection))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'explain', expectedResponse, 0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and failed command events'() {
        given:
        def filter = parse('{_id : {$fakeOp : 1}}')
        def projection = parse('{_id : 1}')
        def skip = 5
        def protocol = new QueryProtocol(getNamespace(), skip, 5, filter, projection, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoQueryException)
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'find',
                                                                     new BsonDocument('find', new BsonString(getCollectionName()))
                                                                             .append('filter', filter)
                                                                             .append('projection', projection)
                                                                             .append('skip', new BsonInt32(skip))),
                                             new CommandFailedEvent(1, connection.getDescription(), 'find', 0, e)])

        where:
        async << [false, true]
    }
}

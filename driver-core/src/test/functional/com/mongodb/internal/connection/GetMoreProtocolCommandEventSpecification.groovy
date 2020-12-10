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

import com.mongodb.MongoQueryException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getCredentialWithCache
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.internal.connection.ProtocolTestHelper.execute

class GetMoreProtocolCommandEventSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialWithCache(), null, null, [], null, getServerApi())
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open();
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should deliver start and completed command events'() {
        given:
        def documents = [new BsonDocument('_id', new BsonInt32(1)),
                         new BsonDocument('_id', new BsonInt32(2)),
                         new BsonDocument('_id', new BsonInt32(3)),
                         new BsonDocument('_id', new BsonInt32(4)),
                         new BsonDocument('_id', new BsonInt32(5))]
        collectionHelper.insertDocuments(documents)
        def result = new QueryProtocol(getNamespace(), 1, 2, new BsonDocument(), null, new BsonDocumentCodec()).execute(connection)
        def protocol = new GetMoreProtocol(getNamespace(), result.cursor.id, 2, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        def getMoreResult = execute(protocol, connection, async)

        then:
        def response = new BsonDocument('cursor',
                                        new BsonDocument('id', getMoreResult.cursor ? new BsonInt64(getMoreResult.cursor.id)
                                                                                    : new BsonInt64(0))
                                                .append('ns', new BsonString(getNamespace().getFullName()))
                                                .append('nextBatch', new BsonArray(documents.subList(3, 5))))
                .append('ok', new BsonDouble(1.0))
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'getMore',
                                                                     new BsonDocument('getMore', new BsonInt64(result.cursor.id))
                                                                             .append('collection', new BsonString(getCollectionName()))
                                                                             .append('batchSize', new BsonInt32(2))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'getMore',
                                                                       response,
                                                                       0)])

        where:
        async << [false, true]
    }

    def 'should deliver start and failed command events'() {
        given:
        collectionHelper.insertDocuments(new Document(), new Document(), new Document(), new Document(), new Document())
        def result = new QueryProtocol(getNamespace(), 1, 2, new BsonDocument(), null, new BsonDocumentCodec())
                .execute(connection)
        new KillCursorProtocol(getNamespace(), [result.cursor.id]).execute(connection)
        def protocol = new GetMoreProtocol(getNamespace(), result.cursor.id, 2, new BsonDocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoQueryException)
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'getMore',
                                                                     new BsonDocument('getMore', new BsonInt64(result.cursor.id))
                                                                             .append('collection', new BsonString(getCollectionName()))
                                                                             .append('batchSize', new BsonInt32(2))),
                                             new CommandFailedEvent(1, connection.getDescription(), 'getMore', 0, e)
        ])

        where:
        async << [false, true]
    }
}

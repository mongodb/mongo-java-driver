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

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.connection.ConnectionFixture.getCredentialListWithCache
import static com.mongodb.connection.ProtocolTestHelper.execute

class KillCursorProtocolCommandEventSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection;

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialListWithCache(), null, null, [], null)
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open();
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should deliver start and completed command events'() {
        given:
        collectionHelper.insertDocuments(new Document(), new Document(), new Document(), new Document(), new Document())
        def result = new QueryProtocol(getNamespace(), 1, 2, new BsonDocument(), null, new BsonDocumentCodec())
                .execute(connection)
        def protocol = new KillCursorProtocol(getNamespace(), [result.cursor.id])

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), getDatabaseName(), 'killCursors',
                                                                     new BsonDocument('killCursors', new BsonString(getCollectionName()))
                                                                                      .append('cursors',
                                                                                              new BsonArray([new BsonInt64(result.cursor
                                                                                                                                 .id)]))),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'killCursors',
                                                                       new BsonDocument('ok', new BsonDouble(1.0))
                                                                               .append('cursorsUnknown',
                                                                                       new BsonArray([new BsonInt64(result.cursor.id)])),
                                                                       0)])

        where:
        async << [false, true]
    }

    def 'should not deliver start and completed command events if no namespace is provided'() {
        given:
        collectionHelper.insertDocuments(new Document(), new Document(), new Document(), new Document(), new Document())
        def result = new QueryProtocol(getNamespace(), 1, 2, new BsonDocument(), null, new BsonDocumentCodec())
                .execute(connection)
        def protocol = new KillCursorProtocol(null, [result.cursor.id])

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([])

        where:
        async << [false, true]
    }
}

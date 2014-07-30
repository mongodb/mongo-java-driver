/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoNamespace
import com.mongodb.ServerCursor
import com.mongodb.event.ConnectionListener
import com.mongodb.operation.QueryFlag
import com.mongodb.protocol.KillCursor
import com.mongodb.protocol.message.CommandMessage
import com.mongodb.protocol.message.KillCursorsMessage
import com.mongodb.protocol.message.MessageSettings
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

import static MongoNamespace.COMMAND_COLLECTION_NAME
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSSLSettings

class InternalStreamConnectionSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    def streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings())
    def stream = streamFactory.create(getPrimary())

    def cleanup() {
        stream.close();
    }

    def 'should fire connection opened event'() {
        given:
        def listener = Mock(ConnectionListener)

        when:
        new InternalStreamConnection(CLUSTER_ID, stream, [], listener)

        then:
        1 * listener.connectionOpened(_)
    }

    def 'should fire connection closed event'() {
        given:
        def listener = Mock(ConnectionListener)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)

        when:
        connection.close()

        then:
        1 * listener.connectionClosed(_)
    }

    def 'should fire messages sent event'() {
        given:
        def listener = Mock(ConnectionListener)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def buffer = new ByteBufferOutputBuffer(connection);
        def message = new KillCursorsMessage(new KillCursor(new ServerCursor(1, getPrimary())));
        message.encode(buffer);

        when:
        connection.sendMessage(buffer.getByteBuffers(), message.getId())

        then:
        1 * listener.messagesSent(_)
    }

    def 'should fire message received event'() {
        given:
        def listener = Mock(ConnectionListener)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def buffer = new ByteBufferOutputBuffer(connection)
        def message = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).fullName,
                                         new BsonDocument('ismaster', new BsonInt32(1)), EnumSet.noneOf(QueryFlag),
                                         MessageSettings.builder().build());
        message.encode(buffer);

        when:
        connection.sendMessage(buffer.getByteBuffers(), message.getId())
        connection.receiveMessage()

        then:
        1 * listener.messageReceived(_)
    }
}

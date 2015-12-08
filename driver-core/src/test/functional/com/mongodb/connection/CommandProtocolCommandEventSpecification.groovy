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
 *
 *
 */

package com.mongodb.connection

import com.mongodb.MongoCommandException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.connection.netty.NettyStreamFactory
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.codecs.DocumentCodec
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getCredentialList
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.connection.ProtocolTestHelper.execute

class CommandProtocolCommandEventSpecification extends OperationFunctionalSpecification {
    @Shared
    InternalStreamConnection connection;

    def setupSpec() {
        connection = new InternalStreamConnectionFactory(new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                                                         getCredentialList(), new NoOpConnectionListener())
                .create(new ServerId(new ClusterId(), getPrimary()))
        connection.open();
    }

    def cleanupSpec() {
        connection?.close()
    }

    def 'should deliver start and completed command events'() {
        given:
        def pingCommand = new BsonDocument('ping', new BsonInt32(1))
        def protocol = new CommandProtocol('admin', pingCommand, new NoOpFieldNameValidator(), new DocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), 'admin', 'ping',
                                                                     pingCommand),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'ping',
                                                                       new BsonDocument('ok', new BsonDouble(1)), 1000)])

        where:
        async << [false, true]
    }

    def 'should deliver start and failed command events'() {
        given:
        def unknownCommand = new BsonDocument('unknown', new BsonInt32(1))
        def protocol = new CommandProtocol('admin', unknownCommand, new NoOpFieldNameValidator(), new DocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoCommandException)
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), 'admin', 'unknown',
                                                                     unknownCommand),
                                             new CommandFailedEvent(1, connection.getDescription(), 'unknown', 0, e)])

        where:
        async << [false, true]
    }

    def 'should deliver start and completed command events for wrapped commands'() {
        given:
        def pingCommand = new BsonDocument('ping', new BsonInt32(1))
        def wrappedCommand = new BsonDocument('$query', pingCommand)
                .append('$readPreference', secondary().toDocument())
        def protocol = new CommandProtocol('admin', wrappedCommand, new NoOpFieldNameValidator(), new DocumentCodec())

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener

        when:
        execute(protocol, connection, async)

        then:
        commandListener.eventsWereDelivered([new CommandStartedEvent(1, connection.getDescription(), 'admin', 'ping',
                                                                     pingCommand),
                                             new CommandSucceededEvent(1, connection.getDescription(), 'ping',
                                                                       new BsonDocument('ok', new BsonDouble(1)), 1000)])

        where:
        async << [false, true]
    }
}

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
 */

package com.mongodb.connection

import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketWriteException
import com.mongodb.ServerAddress
import com.mongodb.bulk.DeleteRequest
import com.mongodb.event.CommandFailedEvent
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.codecs.DocumentCodec
import spock.lang.Shared
import spock.lang.Specification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.connection.ProtocolTestHelper.execute

class CommandEventOnConnectionFailureSpecification extends Specification {

    @Shared
    def namespace = new MongoNamespace('test.test')
    private TestInternalConnection connection;

    def setup() {
        connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress()));
    }

    def 'should publish failed command event when sendMessage throws exception'() {
        String commandName = protocolInfo[0]
        Protocol protocol = protocolInfo[1]

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener
        connection.enqueueSendMessageException(new MongoSocketWriteException('Failure', new ServerAddress(), new IOException()));

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoSocketWriteException)
        commandListener.events.size() == 2
        commandListener.eventWasDelivered(new CommandFailedEvent(1, connection.getDescription(), commandName, 0, e), 1)

        where:
        [protocolInfo, async] << [[
                                   ['killCursors',
                                    new KillCursorProtocol(namespace, [42L])],
                                   ['getMore',
                                    new GetMoreProtocol(namespace, 42L, 1, new DocumentCodec())],
                                   ['find',
                                    new QueryProtocol(namespace, 0, 1, 1, new BsonDocument(), new BsonDocument(), new DocumentCodec())],
                                   ['delete',
                                    new DeleteProtocol(namespace, true, ACKNOWLEDGED,
                                            [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])],
                                   ['delete',
                                    new DeleteProtocol(namespace, true, UNACKNOWLEDGED,
                                            [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])],
                                  ],
                                  [false, true]].combinations()
    }

    def 'should publish failed command event when receiveMessage throws exception'() {
        String commandName = protocolInfo[0]
        Protocol protocol = protocolInfo[1]

        def commandListener = new TestCommandListener()
        protocol.commandListener = commandListener
        connection.enqueueReceiveMessageException(new MongoSocketReadException('Failure', new ServerAddress(), new IOException()));

        when:
        execute(protocol, connection, async)

        then:
        def e = thrown(MongoSocketReadException)
        commandListener.events.size() == 2
        commandListener.eventWasDelivered(new CommandFailedEvent(1, connection.getDescription(), commandName, 0, e), 1)

        where:
        [protocolInfo, async] << [[
                                   ['getMore',
                                    new GetMoreProtocol(namespace, 42L, 1, new DocumentCodec())],
                                   ['find',
                                    new QueryProtocol(namespace, 0, 1, 1, new BsonDocument(), new BsonDocument(), new DocumentCodec())],
                                   ['delete',
                                    new DeleteProtocol(namespace, true, ACKNOWLEDGED,
                                            [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])],
                                  ],
                                  [false, true]].combinations()
    }
}

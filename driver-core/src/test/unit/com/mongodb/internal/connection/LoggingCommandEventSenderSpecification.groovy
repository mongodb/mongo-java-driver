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

import com.mongodb.MongoInternalException
import com.mongodb.MongoNamespace
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerId
import com.mongodb.diagnostics.logging.Logger
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandListener
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.internal.operation.ServerVersionHelper.THREE_DOT_SIX_WIRE_VERSION

class LoggingCommandEventSenderSpecification extends Specification {

    def 'should send events'() {
        given:
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()))
        def namespace = new MongoNamespace('test.driver')
        def messageSettings = MessageSettings.builder().maxWireVersion(THREE_DOT_SIX_WIRE_VERSION).build()
        def commandListener = new TestCommandListener()
        def commandDocument = new BsonDocument('ping', new BsonInt32(1))
        def replyDocument = new BsonDocument('ok', new BsonInt32(1))
        def failureException = new MongoInternalException('failure!')
        def message = new CommandMessage(namespace, commandDocument,
                new NoOpFieldNameValidator(), ReadPreference.primary(), messageSettings, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, NoOpSessionContext.INSTANCE)
        def logger = Stub(Logger) {
            isDebugEnabled() >> debugLoggingEnabled
        }
        def sender = new LoggingCommandEventSender([] as Set, [] as Set, connectionDescription, commandListener, requestContext, message, bsonOutput,
                logger)

        when:
        sender.sendStartedEvent()
        sender.sendSucceededEventForOneWayCommand()
        sender.sendSucceededEvent(MessageHelper.buildSuccessfulReply(message.getId(), replyDocument.toJson()))
        sender.sendFailedEvent(failureException)

        then:
        commandListener.eventsWereDelivered(
                [
                        new CommandStartedEvent(requestContext, message.getId(), connectionDescription, namespace.databaseName,
                                commandDocument.getFirstKey(), commandDocument.append('$db', new BsonString(namespace.databaseName))),
                        new CommandSucceededEvent(requestContext, message.getId(), connectionDescription, commandDocument.getFirstKey(),
                                new BsonDocument(), 1),
                        new CommandSucceededEvent(requestContext, message.getId(), connectionDescription, commandDocument.getFirstKey(),
                                replyDocument, 1),
                        new CommandFailedEvent(requestContext, message.getId(), connectionDescription, commandDocument.getFirstKey(), 1, failureException)
                ])

        where:
        debugLoggingEnabled << [true, false]
    }

    def 'should log events'() {
        given:
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()))
        def namespace = new MongoNamespace('test.driver')
        def messageSettings = MessageSettings.builder().maxWireVersion(THREE_DOT_SIX_WIRE_VERSION).build()
        def commandDocument = new BsonDocument('ping', new BsonInt32(1))
        def replyDocument = new BsonDocument('ok', new BsonInt32(1))
        def failureException = new MongoInternalException('failure!')
        def message = new CommandMessage(namespace, commandDocument, new NoOpFieldNameValidator(), ReadPreference.primary(),
                messageSettings, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, NoOpSessionContext.INSTANCE)
        def logger = Mock(Logger) {
            isDebugEnabled() >> true
        }
        def sender = new LoggingCommandEventSender([] as Set, [] as Set, connectionDescription, commandListener, requestContext, message, bsonOutput,
                logger)
        when:
        sender.sendStartedEvent()
        sender.sendSucceededEventForOneWayCommand()
        sender.sendSucceededEvent(MessageHelper.buildSuccessfulReply(message.getId(), replyDocument.toJson()))
        sender.sendFailedEvent(failureException)

        then:
        1 * logger.debug {
            it == "Sending command \'{\"ping\": 1, \"\$db\": \"test\"}\' with request id ${message.getId()} to database test " +
                    "on connection [connectionId{localValue:${connectionDescription.connectionId.localValue}}] " +
                    'to server 127.0.0.1:27017'
        }
        1 * logger.debug {
            it.matches("Execution of one-way command with request id ${message.getId()} completed successfully in \\d+\\.\\d+ ms " +
                    "on connection \\[connectionId\\{localValue:${connectionDescription.connectionId.localValue}\\}] " +
                    'to server 127\\.0\\.0\\.1:27017')
        }
        1 * logger.debug {
            it.matches("Execution of command with request id ${message.getId()} completed successfully in \\d+\\.\\d+ ms " +
                    "on connection \\[connectionId\\{localValue:${connectionDescription.connectionId.localValue}\\}] " +
                    'to server 127\\.0\\.0\\.1:27017')
        }
        1 * logger.debug({
            it.matches("Execution of command with request id ${message.getId()} failed to complete successfully in \\d+\\.\\d+ ms " +
                    "on connection \\[connectionId\\{localValue:${connectionDescription.connectionId.localValue}\\}] " +
                    'to server 127\\.0\\.0\\.1:27017')
        }, failureException)

        where:
        commandListener << [null, Stub(CommandListener)]
    }

    def 'should log large command with ellipses'() {
        given:
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()))
        def namespace = new MongoNamespace('test.driver')
        def messageSettings = MessageSettings.builder().maxWireVersion(THREE_DOT_SIX_WIRE_VERSION).build()
        def commandDocument = new BsonDocument('fake', new BsonBinary(new byte[2048]))
        def message = new CommandMessage(namespace, commandDocument, new NoOpFieldNameValidator(), ReadPreference.primary(),
                messageSettings, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, NoOpSessionContext.INSTANCE)
        def logger = Mock(Logger) {
            isDebugEnabled() >> true
        }
        def sender = new LoggingCommandEventSender([] as Set, [] as Set, connectionDescription, null, requestContext, message, bsonOutput, logger)

        when:
        sender.sendStartedEvent()

        then:
        1 * logger.debug {
            it == "Sending command \'{\"fake\": {\"\$binary\": {\"base64\": \"${'A' * 967} ...\' " +
                    "with request id ${message.getId()} to database test " +
                    "on connection [connectionId{localValue:${connectionDescription.connectionId.localValue}}] " +
                    'to server 127.0.0.1:27017'
        }
    }

    def 'should log redacted command with ellipses'() {
        given:
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()))
        def namespace = new MongoNamespace('test.driver')
        def messageSettings = MessageSettings.builder().maxWireVersion(THREE_DOT_SIX_WIRE_VERSION).build()
        def commandDocument = new BsonDocument('createUser', new BsonString('private'))
        def message = new CommandMessage(namespace, commandDocument, new NoOpFieldNameValidator(), ReadPreference.primary(),
                messageSettings, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, NoOpSessionContext.INSTANCE)
        def logger = Mock(Logger) {
            isDebugEnabled() >> true
        }
        def sender = new LoggingCommandEventSender(['createUser'] as Set, [] as Set, connectionDescription, null, requestContext, message, bsonOutput,
                logger)

        when:
        sender.sendStartedEvent()

        then:
        1 * logger.debug {
            it == "Sending command \'{\"createUser\": ...\' " +
                    "with request id ${message.getId()} to database test " +
                    "on connection [connectionId{localValue:${connectionDescription.connectionId.localValue}}] " +
                    'to server 127.0.0.1:27017'
        }
    }
}

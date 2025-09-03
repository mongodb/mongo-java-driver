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

import com.mongodb.LoggerSettings
import com.mongodb.MongoInternalException
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandListener
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.diagnostics.logging.Logger
import com.mongodb.internal.logging.StructuredLogger
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION

class LoggingCommandEventSenderSpecification extends Specification {

    def 'should send events'() {
        given:
        def connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()))
        def database = 'test'
        def messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build()
        def commandListener = new TestCommandListener()
        def commandDocument = new BsonDocument('ping', new BsonInt32(1))
        def replyDocument = new BsonDocument('ok', new BsonInt32(1))
        def failureException = new MongoInternalException('failure!')
        def message = new CommandMessage(database, commandDocument,
                NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), messageSettings, MULTIPLE, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                Stub(TimeoutContext), null))
        def logger = Stub(Logger) {
            isDebugEnabled() >> debugLoggingEnabled
        }
        def operationContext = OPERATION_CONTEXT
        def sender = new LoggingCommandEventSender([] as Set, [] as Set, connectionDescription, commandListener,
                operationContext, message, bsonOutput, new StructuredLogger(logger), LoggerSettings.builder().build())

        when:
        sender.sendStartedEvent()
        sender.sendSucceededEventForOneWayCommand()
        sender.sendSucceededEvent(MessageHelper.buildSuccessfulReply(message.getId(), replyDocument.toJson()))
        sender.sendFailedEvent(failureException)

        then:
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, operationContext.id, message.getId(), connectionDescription,
                        database, commandDocument.getFirstKey(),
                        commandDocument.append('$db', new BsonString(database))),
                new CommandSucceededEvent(null, operationContext.id, message.getId(), connectionDescription,
                        database, commandDocument.getFirstKey(), new BsonDocument(), 1),
                new CommandSucceededEvent(null, operationContext.id, message.getId(), connectionDescription,
                        database, commandDocument.getFirstKey(), replyDocument, 1),
                new CommandFailedEvent(null, operationContext.id, message.getId(), connectionDescription,
                        database, commandDocument.getFirstKey(), 1, failureException)
        ])

        where:
        debugLoggingEnabled << [true, false]
    }

    def 'should log events'() {
        given:
        def serverId = new ServerId(new ClusterId(), new ServerAddress())
        def connectionDescription = new ConnectionDescription(serverId)
                .withConnectionId(new ConnectionId(serverId, 42, 1000))
        def database = 'test'
        def messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build()
        def commandDocument = new BsonDocument('ping', new BsonInt32(1))
        def replyDocument = new BsonDocument('ok', new BsonInt32(42))
        def failureException = new MongoInternalException('failure!')
        def message = new CommandMessage(database, commandDocument, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(),
                messageSettings, MULTIPLE, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                Stub(TimeoutContext), null))
        def logger = Mock(Logger) {
            isDebugEnabled() >> true
        }
        def operationContext = OPERATION_CONTEXT
        def sender = new LoggingCommandEventSender([] as Set, [] as Set, connectionDescription, commandListener,
                operationContext, message, bsonOutput, new StructuredLogger(logger),
                LoggerSettings.builder().build())
        when:
        sender.sendStartedEvent()
        sender.sendSucceededEventForOneWayCommand()
        sender.sendSucceededEvent(MessageHelper.buildSuccessfulReply(message.getId(), replyDocument.toJson()))
        sender.sendFailedEvent(failureException)

        then:
        1 * logger.debug {
            it == "Command \"ping\" started on database \"test\" using a connection with driver-generated ID " +
                    "${connectionDescription.connectionId.localValue} and server-generated ID " +
                    "${connectionDescription.connectionId.serverValue} to 127.0.0.1:27017. The " +
                    "request ID is ${message.getId()} and the operation ID is ${operationContext.getId()}. " +
                    "Command: {\"ping\": 1, " + "\"\$db\": \"test\"}"
        }
        1 * logger.debug {
            it.matches("Command \"ping\" succeeded on database \"test\" in \\d+\\.\\d+ ms using a connection with driver-generated ID " +
                    "${connectionDescription.connectionId.localValue} and server-generated ID " +
                    "${connectionDescription.connectionId.serverValue} to 127.0.0.1:27017. The " +
                    "request ID is ${message.getId()} and the operation ID is ${operationContext.getId()}. Command reply: \\{\"ok\": 1}")
        }
        1 * logger.debug {
            it.matches("Command \"ping\" succeeded on database \"test\" in \\d+\\.\\d+ ms using a connection with driver-generated ID " +
                    "${connectionDescription.connectionId.localValue} and server-generated ID " +
                    "${connectionDescription.connectionId.serverValue} to 127.0.0.1:27017. The " +
                    "request ID is ${message.getId()} and the operation ID is ${operationContext.getId()}. Command reply: \\{\"ok\": 42}")
        }
        1 * logger.debug({
            it.matches("Command \"ping\" failed on database \"test\" in \\d+\\.\\d+ ms using a connection with driver-generated ID " +
                    "${connectionDescription.connectionId.localValue} and server-generated ID " +
                    "${connectionDescription.connectionId.serverValue} to 127.0.0.1:27017. The " +
                    "request ID is ${message.getId()} and the operation ID is ${operationContext.getId()}.")
       }, failureException)

        where:
        commandListener << [null, Stub(CommandListener)]
    }

    def 'should log large command with ellipses'() {
        given:
        def serverId = new ServerId(new ClusterId(), new ServerAddress())
        def connectionDescription = new ConnectionDescription(serverId)
                .withConnectionId(new ConnectionId(serverId, 42, 1000))
        def database = 'test'
        def messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build()
        def commandDocument = new BsonDocument('fake', new BsonBinary(new byte[2048]))
        def message = new CommandMessage(database, commandDocument, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(),
                messageSettings, SINGLE, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                Stub(TimeoutContext), null))
        def logger = Mock(Logger) {
            isDebugEnabled() >> true
        }
        def operationContext = OPERATION_CONTEXT

        def sender = new LoggingCommandEventSender([] as Set, [] as Set, connectionDescription, null, operationContext,
                message, bsonOutput, new StructuredLogger(logger), LoggerSettings.builder().build())

        when:
        sender.sendStartedEvent()

        then:
        1 * logger.debug {
            it == "Command \"fake\" started on database \"test\" using a connection with driver-generated ID " +
                    "${connectionDescription.connectionId.localValue} and server-generated ID " +
                    "${connectionDescription.connectionId.serverValue} to 127.0.0.1:27017. The " +
                    "request ID is ${message.getId()} and the operation ID is ${operationContext.getId()}. " +
                    "Command: {\"fake\": {\"\$binary\": {\"base64\": \"${'A' * 967} ..."
        }
    }

    def 'should log redacted command with ellipses'() {
        given:
        def serverId = new ServerId(new ClusterId(), new ServerAddress())
        def connectionDescription = new ConnectionDescription(serverId)
                .withConnectionId(new ConnectionId(serverId, 42, 1000))
        def database = 'test'
        def messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build()
        def commandDocument = new BsonDocument('createUser', new BsonString('private'))
        def message = new CommandMessage(database, commandDocument, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(),
                messageSettings, SINGLE, null)
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                Stub(TimeoutContext), null))
        def logger = Mock(Logger) {
            isDebugEnabled() >> true
        }
        def operationContext = OPERATION_CONTEXT
        def sender = new LoggingCommandEventSender(['createUser'] as Set, [] as Set, connectionDescription, null,
                operationContext, message, bsonOutput, new StructuredLogger(logger), LoggerSettings.builder().build())

        when:
        sender.sendStartedEvent()

        then:
        1 * logger.debug {
            it == "Command \"createUser\" started on database \"test\" using a connection with driver-generated ID " +
                    "${connectionDescription.connectionId.localValue} and server-generated ID " +
                    "${connectionDescription.connectionId.serverValue} to 127.0.0.1:27017. The " +
                    "request ID is ${message.getId()} and the operation ID is ${operationContext.getId()}. Command: {}"
        }
    }
}

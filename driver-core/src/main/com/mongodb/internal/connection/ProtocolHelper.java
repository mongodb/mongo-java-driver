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

package com.mongodb.internal.connection;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;
import org.bson.io.ByteBufferBsonInput;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.codecs.BsonValueCodecProvider.getClassForBsonType;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class ProtocolHelper {
    private static final Logger PROTOCOL_EVENT_LOGGER = Loggers.getLogger("protocol.event");
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final int NO_ERROR_CODE = -1;


    static boolean isCommandOk(final BsonDocument response) {
        BsonValue okValue = response.get("ok");
        return isCommandOk(okValue);
    }

    static boolean isCommandOk(final BsonReader bsonReader) {
        return isCommandOk(getField(bsonReader, "ok"));
    }

    static boolean isCommandOk(final ResponseBuffers responseBuffers) {
        try {
            return isCommandOk(createBsonReader(responseBuffers));
        } finally {
            responseBuffers.reset();
        }
    }

    static MongoException createSpecialWriteConcernException(final ResponseBuffers responseBuffers, final ServerAddress serverAddress) {
        BsonValue writeConcernError = getField(createBsonReader(responseBuffers), "writeConcernError");
        if (writeConcernError == null) {
            return null;
        } else {
            return createSpecialException(writeConcernError.asDocument(), serverAddress, "errmsg");
        }
    }

    static BsonTimestamp getOperationTime(final ResponseBuffers responseBuffers) {
        try {
            BsonValue operationTime = getField(createBsonReader(responseBuffers), "operationTime");
            if (operationTime == null) {
                return null;
            }
            return operationTime.asTimestamp();
        } finally {
            responseBuffers.reset();
        }
    }

    static BsonDocument getClusterTime(final ResponseBuffers responseBuffers) {
        BsonValue value = getFieldValue(responseBuffers, "$clusterTime");
        if (value == null) {
            return null;
        }
        return value.asDocument();
    }

    static BsonDocument getClusterTime(final BsonDocument response) {
        BsonValue clusterTime = response.get("$clusterTime");
        if (clusterTime == null) {
            return null;
        }
        return clusterTime.asDocument();
    }

    static BsonTimestamp getSnapshotTimestamp(final ResponseBuffers responseBuffers) {
        BsonValue atClusterTimeValue = getNestedFieldValue(responseBuffers, "cursor", "atClusterTime");
        if (atClusterTimeValue == null) {
            atClusterTimeValue = getFieldValue(responseBuffers, "atClusterTime");
        }
        if (atClusterTimeValue != null && atClusterTimeValue.getBsonType() == BsonType.TIMESTAMP) {
            return atClusterTimeValue.asTimestamp();
        }
        return null;
    }

    static BsonDocument getRecoveryToken(final ResponseBuffers responseBuffers) {
        BsonValue value = getFieldValue(responseBuffers, "recoveryToken");
        if (value == null) {
            return null;
        }
        return value.asDocument();
    }

    private static BsonValue getFieldValue(final ResponseBuffers responseBuffers, final String fieldName) {
        try {
            return getField(createBsonReader(responseBuffers), fieldName);
        } finally {
            responseBuffers.reset();
        }
    }

    private static BsonBinaryReader createBsonReader(final ResponseBuffers responseBuffers) {
        return new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer()));
    }


    private static BsonValue getField(final BsonReader bsonReader, final String fieldName) {
        bsonReader.readStartDocument();
        while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (bsonReader.readName().equals(fieldName)) {
                return REGISTRY.get(getClassForBsonType(bsonReader.getCurrentBsonType())).decode(bsonReader,
                        DecoderContext.builder().build());
            }
            bsonReader.skipValue();
        }
        bsonReader.readEndDocument();
        return null;
    }

    @SuppressWarnings("SameParameterValue")
    private static BsonValue getNestedFieldValue(final ResponseBuffers responseBuffers, final String topLevelFieldName,
                                                 final String nestedFieldName) {
        try {
            BsonReader bsonReader = createBsonReader(responseBuffers);
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (bsonReader.readName().equals(topLevelFieldName)) {
                    return getField(bsonReader, nestedFieldName);
                }
                bsonReader.skipValue();
            }
            bsonReader.readEndDocument();
            return null;
        } finally {
            responseBuffers.reset();
        }
    }

    private static boolean isCommandOk(final BsonValue okValue) {
        if (okValue == null) {
            return false;
        } else if (okValue.isBoolean()) {
            return okValue.asBoolean().getValue();
        } else if (okValue.isNumber()) {
            return okValue.asNumber().intValue() == 1;
        } else {
            return false;
        }
    }

    static MongoException getCommandFailureException(final BsonDocument response, final ServerAddress serverAddress) {
        MongoException specialException = createSpecialException(response, serverAddress, "errmsg");
        if (specialException != null) {
            return specialException;
        }
        return new MongoCommandException(response, serverAddress);
    }

    static int getErrorCode(final BsonDocument response) {
        return (response.getNumber("code", new BsonInt32(-1)).intValue());
    }

    static String getErrorMessage(final BsonDocument response, final String errorMessageFieldName) {
        return response.getString(errorMessageFieldName, new BsonString("")).getValue();
    }

    static MongoException getQueryFailureException(final BsonDocument errorDocument, final ServerAddress serverAddress) {
        MongoException specialException = createSpecialException(errorDocument, serverAddress, "$err");
        if (specialException != null) {
            return specialException;
        }
        return new MongoQueryException(serverAddress, getErrorCode(errorDocument), getErrorMessage(errorDocument, "$err"));
    }

    static MessageSettings getMessageSettings(final ConnectionDescription connectionDescription) {
        return MessageSettings.builder()
                .maxDocumentSize(connectionDescription.getMaxDocumentSize())
                .maxMessageSize(connectionDescription.getMaxMessageSize())
                .maxBatchCount(connectionDescription.getMaxBatchCount())
                .maxWireVersion(connectionDescription.getMaxWireVersion())
                .serverType(connectionDescription.getServerType())
                .build();
    }

    static void encodeMessage(final RequestMessage message, final BsonOutput bsonOutput) {
        try {
            message.encode(bsonOutput, NoOpSessionContext.INSTANCE);
        } catch (RuntimeException e) {
            bsonOutput.close();
            throw e;
        } catch (Error e) {
            bsonOutput.close();
            throw e;
        }
    }

    static RequestMessage.EncodingMetadata encodeMessageWithMetadata(final RequestMessage message, final BsonOutput bsonOutput) {
        try {
            message.encode(bsonOutput, NoOpSessionContext.INSTANCE);
            return message.getEncodingMetadata();
        } catch (RuntimeException e) {
            bsonOutput.close();
            throw e;
        } catch (Error e) {
            bsonOutput.close();
            throw e;
        }
    }

    private static final List<Integer> NOT_PRIMARY_CODES = asList(10107, 13435, 10058);
    private static final List<String> NOT_PRIMARY_MESSAGES = singletonList("not master");
    private static final List<Integer> RECOVERING_CODES = asList(11600, 11602, 13436, 189, 91);
    private static final List<String> RECOVERING_MESSAGES = asList("not master or secondary", "node is recovering");
    public static MongoException createSpecialException(final BsonDocument response, final ServerAddress serverAddress,
                                                        final String errorMessageFieldName) {
        if (response == null) {
            return null;
        }
        int errorCode = getErrorCode(response);
        String errorMessage = getErrorMessage(response, errorMessageFieldName);
        if (ErrorCategory.fromErrorCode(errorCode) == ErrorCategory.EXECUTION_TIMEOUT) {
            return new MongoExecutionTimeoutException(errorCode, errorMessage, response);
        } else if (isNodeIsRecoveringError(errorCode, errorMessage)) {
            return new MongoNodeIsRecoveringException(response, serverAddress);
        } else if (isNotPrimaryError(errorCode, errorMessage)) {
            return new MongoNotPrimaryException(response, serverAddress);
        } else if (response.containsKey("writeConcernError")) {
            return createSpecialException(response.getDocument("writeConcernError"), serverAddress, "errmsg");
        } else {
            return null;
        }
    }

    private static boolean isNotPrimaryError(final int errorCode, final String errorMessage) {
        return NOT_PRIMARY_CODES.contains(errorCode)
                || (errorCode == NO_ERROR_CODE && NOT_PRIMARY_MESSAGES.stream().anyMatch(errorMessage::contains));
    }

    private static boolean isNodeIsRecoveringError(final int errorCode, final String errorMessage) {
        return RECOVERING_CODES.contains(errorCode)
                || (errorCode == NO_ERROR_CODE && (RECOVERING_MESSAGES.stream().anyMatch(errorMessage::contains)));
    }

    static void sendCommandStartedEvent(final RequestMessage message, final String databaseName, final String commandName,
                                        final BsonDocument command, final ConnectionDescription connectionDescription,
                                        final CommandListener commandListener) {
        try {
            commandListener.commandStarted(new CommandStartedEvent(message.getId(), connectionDescription,
                                                                   databaseName, commandName, command));
        } catch (Exception e) {
            if (PROTOCOL_EVENT_LOGGER.isWarnEnabled()) {
                PROTOCOL_EVENT_LOGGER.warn(format("Exception thrown raising command started event to listener %s", commandListener), e);
            }
        }
    }

    static void sendCommandSucceededEvent(final RequestMessage message, final String commandName, final BsonDocument response,
                                          final ConnectionDescription connectionDescription, final long elapsedTimeNanos,
                                          final CommandListener commandListener) {
        try {
            commandListener.commandSucceeded(new CommandSucceededEvent(message.getId(), connectionDescription, commandName, response,
                    elapsedTimeNanos));
        } catch (Exception e) {
            if (PROTOCOL_EVENT_LOGGER.isWarnEnabled()) {
                PROTOCOL_EVENT_LOGGER.warn(format("Exception thrown raising command succeeded event to listener %s", commandListener), e);
            }
        }
    }

    static void sendCommandFailedEvent(final RequestMessage message, final String commandName,
                                       final ConnectionDescription connectionDescription, final long elapsedTimeNanos,
                                       final Throwable throwable, final CommandListener commandListener) {
        try {
            commandListener.commandFailed(new CommandFailedEvent(message.getId(), connectionDescription, commandName, elapsedTimeNanos,
                    throwable));
        } catch (Exception e) {
            if (PROTOCOL_EVENT_LOGGER.isWarnEnabled()) {
                PROTOCOL_EVENT_LOGGER.warn(format("Exception thrown raising command failed event to listener %s", commandListener), e);
            }
        }
    }


    private ProtocolHelper() {
    }
}

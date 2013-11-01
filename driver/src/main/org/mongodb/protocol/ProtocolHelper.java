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

package org.mongodb.protocol;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoException;
import org.mongodb.MongoExecutionTimeoutException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.MongoWriteException;
import org.mongodb.WriteResult;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.Arrays;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

final class ProtocolHelper {
    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);
    private static final List<Integer> EXECUTION_TIMEOUT_ERROR_CODES = Arrays.asList(50);


    static WriteResult getWriteResult(final CommandResult commandResult) {
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        String errorMessage = commandResult.getResponse().getString("err");
        if (errorMessage != null) {
            if (DUPLICATE_KEY_ERROR_CODES.contains(commandResult.getErrorCode())) {
                throw new MongoDuplicateKeyException(commandResult.getErrorCode(), errorMessage,
                                                     commandResult);
            } else {
                throw new MongoWriteException(commandResult.getErrorCode(), errorMessage, commandResult);
            }
        }

        Boolean updatedExisting = commandResult.getResponse().getBoolean("updatedExisting");

        return new AcknowledgedWriteResult(((Number) commandResult.getResponse().get("n")).intValue(),
                               updatedExisting != null ? updatedExisting : false, commandResult.getResponse().get("upserted"));
    }

    static MongoException getCommandFailureException(final CommandResult commandResult) {
        isTrue("not ok", !commandResult.isOk());
        if (EXECUTION_TIMEOUT_ERROR_CODES.contains(commandResult.getErrorCode())) {
            return new MongoExecutionTimeoutException(commandResult.getAddress(), commandResult.getErrorCode(),
                                                      commandResult.getErrorMessage());
        }
        return new MongoCommandFailureException(commandResult);
    }

    static MongoException getQueryFailureException(final ServerAddress serverAddress, final Document errorDocument) {
        if (EXECUTION_TIMEOUT_ERROR_CODES.contains(getErrorCode(errorDocument))) {
            return new MongoExecutionTimeoutException(serverAddress, getErrorCode(errorDocument), getErrorMessage(errorDocument));
        }
        return new MongoQueryFailureException(serverAddress, getErrorCode(errorDocument), getErrorMessage(errorDocument));
    }

    static String getErrorMessage(final Document errorDocument) {
        return (String) errorDocument.get("$err");
    }

    static int getErrorCode(final Document errorDocument) {
        return (Integer) errorDocument.get("code");
    }

    static MessageSettings getMessageSettings(final ServerDescription serverDescription) {
        return MessageSettings.builder()
                              .maxDocumentSize(serverDescription.getMaxDocumentSize())
                              .maxMessageSize(serverDescription
                                              .getMaxMessageSize())
                              .build();
    }

    static RequestMessage encodeMessageToBuffer(final RequestMessage message, final PooledByteBufferOutputBuffer buffer) {
        try {
            return message.encode(buffer);
        } catch (RuntimeException e) {
            buffer.close();
            throw e;
        } catch (Error e) {
            buffer.close();
            throw e;
        }
    }

    private ProtocolHelper() {
    }
}

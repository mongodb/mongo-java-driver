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

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoException;
import org.mongodb.MongoExecutionTimeoutException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.MongoWriteException;
import org.mongodb.WriteResult;
import org.mongodb.connection.ByteBufferOutputBuffer;
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

    static <T> WriteResult getWriteResult(final CommandResult<T> commandResult) {
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        if (hasWriteError(commandResult.getRawResponse())) {
            throwWriteException(commandResult);
        }

        BsonBoolean updatedExisting = commandResult.getRawResponse().getBoolean("updatedExisting", BsonBoolean.FALSE);

        return new AcknowledgedWriteResult(commandResult.getRawResponse().getNumber("n").intValue(),
                                           updatedExisting.getValue(), commandResult.getRawResponse().get("upserted"));
    }

    static <T> MongoException getCommandFailureException(final CommandResult<T> commandResult) {
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
                              .maxMessageSize(serverDescription.getMaxMessageSize())
                              .maxWriteBatchSize(serverDescription.getMaxWriteBatchSize())
                              .build();
    }

    static RequestMessage encodeMessageToBuffer(final RequestMessage message, final ByteBufferOutputBuffer buffer) {
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

    private static boolean hasWriteError(final BsonDocument response) {
        String err = getWriteErrorMessage(response);
        return err != null && err.length() > 0;
    }

    private static String getWriteErrorMessage(final BsonDocument response) {
        if (response.isString("err")) {
            return response.getString("err").getValue();
        } else {
            return null;
        }
    }

    private static <T> void throwWriteException(final CommandResult<T> commandResult) {
        int code = getCode(commandResult.getRawResponse());
        if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
            throw new MongoDuplicateKeyException(code, getWriteErrorMessage(commandResult.getRawResponse()),
                                                 commandResult.getRawResponse(), commandResult.getAddress());
        } else {
            throw new MongoWriteException(code, getWriteErrorMessage(commandResult.getRawResponse()),
                                          commandResult.getRawResponse(), commandResult.getAddress());
        }
    }

    @SuppressWarnings("unchecked")
    private static int getCode(final BsonDocument response) {
        // mongos may return a list of documents representing getlasterror responses from each shard.  Return the one with a matching
        // "err" field, so that it can be used to get the error code
        if (!response.containsKey("code") && response.containsKey("errObjects")) {
            for (BsonValue curErrorDocument : response.getArray("errObjects")) {
                if (getWriteErrorMessage(response).equals(getWriteErrorMessage(curErrorDocument.asDocument()))) {
                    return curErrorDocument.asDocument().getNumber("code").intValue();
                }
            }
        }

        return response.getNumber("code", new BsonInt32(-1)).intValue();
    }

    private ProtocolHelper() {
    }
}

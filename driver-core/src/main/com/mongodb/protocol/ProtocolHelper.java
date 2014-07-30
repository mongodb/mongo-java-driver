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

package com.mongodb.protocol;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoQueryFailureException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernException;
import com.mongodb.connection.ByteBufferOutputBuffer;
import com.mongodb.connection.ServerDescription;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;

final class ProtocolHelper {
    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);
    private static final List<Integer> EXECUTION_TIMEOUT_ERROR_CODES = Arrays.asList(50);

    static WriteResult getWriteResult(final CommandResult commandResult) {
        if (!commandResult.isOk()) {
            throw new CommandFailureException(commandResult.getResponse(), commandResult.getAddress());
        }

        if (hasWriteError(commandResult.getResponse())) {
            throwWriteException(commandResult);
        }

        return createWriteResult(commandResult);
    }

    private static WriteResult createWriteResult(final CommandResult commandResult) {
        BsonBoolean updatedExisting = commandResult.getResponse().getBoolean("updatedExisting", BsonBoolean.FALSE);

        return new AcknowledgedWriteResult(commandResult.getResponse().getNumber("n", new BsonInt32(0)).intValue(),
                                           updatedExisting.getValue(), commandResult.getResponse().get("upserted"));
    }

    static MongoException getCommandFailureException(final CommandResult commandResult) {
        isTrue("not ok", !commandResult.isOk());
        if (EXECUTION_TIMEOUT_ERROR_CODES.contains(commandResult.getErrorCode())) {
            return new MongoExecutionTimeoutException(commandResult.getErrorCode(), commandResult.getErrorMessage());
        }
        return new CommandFailureException(commandResult.getResponse(), commandResult.getAddress());
    }

    static MongoException getQueryFailureException(final ServerAddress serverAddress, final Document errorDocument) {
        if (EXECUTION_TIMEOUT_ERROR_CODES.contains(getErrorCode(errorDocument))) {
            return new MongoExecutionTimeoutException(getErrorCode(errorDocument), getErrorMessage(errorDocument));
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
        String err = MongoWriteException.extractErrorMessage(response);
        return err != null && err.length() > 0;
    }

    private static com.mongodb.WriteResult getWriteResult2(final CommandResult commandResult) {
        WriteResult writeResult = createWriteResult(commandResult);
        // TODO: translate upsertedId
        return new com.mongodb.WriteResult(writeResult.getCount(), writeResult.isUpdateOfExisting(), writeResult.getUpsertedId());
    }

    private static void throwWriteException(final CommandResult commandResult) {
        int code = MongoWriteException.extractErrorCode(commandResult.getResponse());
        if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
            throw new MongoException.DuplicateKey(commandResult.getResponse(), commandResult.getAddress(), getWriteResult2(commandResult));
        } else {
            throw new WriteConcernException(commandResult.getResponse(), commandResult.getAddress(), getWriteResult2(commandResult));
        }
    }

    private ProtocolHelper() {
    }
}

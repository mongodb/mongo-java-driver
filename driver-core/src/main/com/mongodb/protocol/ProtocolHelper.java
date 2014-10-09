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
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.io.BsonOutput;
import org.mongodb.WriteResult;

import java.util.Arrays;
import java.util.List;

final class ProtocolHelper {
    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);
    private static final List<Integer> EXECUTION_TIMEOUT_ERROR_CODES = Arrays.asList(50);

    static WriteResult getWriteResult(final BsonDocument result, final ServerAddress serverAddress) {
        if (!isCommandOk(result)) {
            throw new CommandFailureException(result, serverAddress);
        }

        if (hasWriteError(result)) {
            throwWriteException(result, serverAddress);
        }

        return createWriteResult(result);
    }

    private static WriteResult createWriteResult(final BsonDocument result) {
        BsonBoolean updatedExisting = result.getBoolean("updatedExisting", BsonBoolean.FALSE);

        return new AcknowledgedWriteResult(result.getNumber("n", new BsonInt32(0)).intValue(),
                                           updatedExisting.getValue(), result.get("upserted"));
    }


    static boolean isCommandOk(final BsonDocument response) {
        BsonValue okValue = response.get("ok");
        if (okValue.isBoolean()) {
            return okValue.asBoolean().getValue();
        } else if (okValue.isNumber()) {
            return okValue.asNumber().intValue() == 1;
        } else {
            return false;
        }
    }

    static MongoException getCommandFailureException(final BsonDocument response, final ServerAddress serverAddress) {
        if (EXECUTION_TIMEOUT_ERROR_CODES.contains(getErrorCode(response))) {
            return new MongoExecutionTimeoutException(getErrorCode(response), getErrorMessage(response));
        }
        return new CommandFailureException(response, serverAddress);
    }

    static int getErrorCode(final BsonDocument response) {
        return (response.getNumber("code", new BsonInt32(-1)).intValue());
    }

    static String getErrorMessage(final BsonDocument response) {
        return response.getString("errmsg", null).getValue();
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

    static MessageSettings getMessageSettings(final ConnectionDescription connectionDescription) {
        return MessageSettings.builder()
                              .maxDocumentSize(connectionDescription.getMaxDocumentSize())
                              .maxMessageSize(connectionDescription.getMaxMessageSize())
                              .maxBatchCount(connectionDescription.getMaxBatchCount())
                              .build();
    }

    static RequestMessage encodeMessage(final RequestMessage message, final BsonOutput bsonOutput) {
        try {
            return message.encode(bsonOutput);
        } catch (RuntimeException e) {
            bsonOutput.close();
            throw e;
        } catch (Error e) {
            bsonOutput.close();
            throw e;
        }
    }

    private static boolean hasWriteError(final BsonDocument response) {
        String err = MongoWriteException.extractErrorMessage(response);
        return err != null && err.length() > 0;
    }

    private static com.mongodb.WriteResult getWriteResult(final BsonDocument result) {
        WriteResult writeResult = createWriteResult(result);
        // TODO: translate upsertedId
        return new com.mongodb.WriteResult(writeResult.getCount(), writeResult.isUpdateOfExisting(), writeResult.getUpsertedId());
    }

    @SuppressWarnings("deprecation")
    private static void throwWriteException(final BsonDocument result, final ServerAddress serverAddress) {
        int code = MongoWriteException.extractErrorCode(result);
        if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
            throw new MongoException.DuplicateKey(result, serverAddress, getWriteResult(result));
        } else {
            throw new WriteConcernException(result, serverAddress, getWriteResult(result));
        }
    }

    private ProtocolHelper() {
    }
}

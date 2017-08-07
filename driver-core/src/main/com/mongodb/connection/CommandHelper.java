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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoServerException;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

final class CommandHelper {
    static BsonDocument executeCommand(final String database, final BsonDocument command, final InternalConnection internalConnection) {
        return sendAndReceive(database, command, internalConnection);
    }

    static BsonDocument executeCommandWithoutCheckingForFailure(final String database, final BsonDocument command,
                                                                final InternalConnection internalConnection) {
        try {
            return sendAndReceive(database, command, internalConnection);
        } catch (MongoServerException e) {
            return new BsonDocument();
        }
    }

    static void executeCommandAsync(final String database, final BsonDocument command, final InternalConnection internalConnection,
                                    final SingleResultCallback<BsonDocument> callback) {
        final SimpleCommandMessage message =
                new SimpleCommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME).getFullName(), command, false,
                                                MessageSettings.builder().build());

        internalConnection.sendAndReceiveAsync(message, new SingleResultCallback<ResponseBuffers>() {
            @Override
            public void onResult(final ResponseBuffers result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(result, new BsonDocumentCodec(),
                                                                                                    message.getId());
                    BsonDocument reply = replyMessage.getDocuments().get(0);
                    callback.onResult(reply, null);
                }
            }
        });
    }

    static boolean isCommandOk(final BsonDocument response) {
        if (!response.containsKey("ok")) {
            return false;
        }
        BsonValue okValue = response.get("ok");
        if (okValue.isBoolean()) {
            return okValue.asBoolean().getValue();
        } else if (okValue.isNumber()) {
            return okValue.asNumber().intValue() == 1;
        } else {
            return false;
        }
    }

    private static BsonDocument sendAndReceive(final String database, final BsonDocument command,
                                               final InternalConnection internalConnection) {
        SimpleCommandMessage message = new SimpleCommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME).getFullName(),
                                                                       command, false, MessageSettings.builder().build());
        ResponseBuffers responseBuffers = internalConnection.sendAndReceive(message);
        try {
            return new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), message.getId()).getDocuments().get(0);
        } finally {
            responseBuffers.close();
        }
    }

    private CommandHelper() {
    }
}

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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static java.lang.String.format;

final class CommandHelper {
    static BsonDocument executeCommand(final String database, final BsonDocument command, final InternalConnection internalConnection) {
        return receiveCommandResult(internalConnection, sendMessage(database, command, internalConnection));
    }

    static void executeCommandAsync(final String database, final BsonDocument command, final InternalConnection internalConnection,
                                    final SingleResultCallback<BsonDocument> callback) {
        sendMessageAsync(database, command, internalConnection, new SingleResultCallback<SimpleCommandMessage>() {
            @Override
            public void onResult(final SimpleCommandMessage result, final Throwable t) {
                  if (t != null) {
                      callback.onResult(null, t);
                  } else {
                      receiveReplyAsync(internalConnection, result, new SingleResultCallback<ReplyMessage<BsonDocument>>() {
                          @Override
                          public void onResult(final ReplyMessage<BsonDocument> result, final Throwable t) {
                              if (t != null) {
                                  callback.onResult(null, t);
                              } else {
                                  BsonDocument reply = result.getDocuments().get(0);
                                  if (!isCommandOk(reply)) {
                                      callback.onResult(null, createCommandFailureException(reply, internalConnection));
                                  } else {
                                      callback.onResult(reply, null);
                                  }
                              }
                          }
                      });
                  }
            }
        });
    }

    static BsonDocument executeCommandWithoutCheckingForFailure(final String database, final BsonDocument command,
                                                                final InternalConnection internalConnection) {
        return receiveCommandDocument(internalConnection, sendMessage(database, command, internalConnection));
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

    private static SimpleCommandMessage sendMessage(final String database, final BsonDocument command,
                                                    final InternalConnection internalConnection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(internalConnection);
        try {
            SimpleCommandMessage message = new SimpleCommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME).getFullName(),
                                                        command, false, MessageSettings.builder().build());
            message.encode(bsonOutput);
            internalConnection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }

    private static void sendMessageAsync(final String database, final BsonDocument command,
                                         final InternalConnection internalConnection,
                                         final SingleResultCallback<SimpleCommandMessage> callback) {
        final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(internalConnection);
        try {
            final SimpleCommandMessage message =
                    new SimpleCommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME).getFullName(), command, false,
                                                    MessageSettings.builder().build());
            message.encode(bsonOutput);
            internalConnection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    bsonOutput.close();
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(message, null);
                    }
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private static BsonDocument receiveCommandResult(final InternalConnection internalConnection, final SimpleCommandMessage message) {
        BsonDocument result = receiveReply(internalConnection, message).getDocuments().get(0);
        if (!isCommandOk(result)) {
            throw createCommandFailureException(result, internalConnection);
        }

        return result;
    }

    private static BsonDocument receiveCommandDocument(final InternalConnection internalConnection, final CommandMessage message) {
        return receiveReply(internalConnection, message).getDocuments().get(0);
    }

    private static ReplyMessage<BsonDocument> receiveReply(final InternalConnection internalConnection, final CommandMessage message) {
        ResponseBuffers responseBuffers = internalConnection.receiveMessage(message.getId());
        if (responseBuffers == null) {
            throw new MongoInternalException(format("Response buffers received from %s should not be null", internalConnection));
        }
        try {
            return new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), message.getId());
        } finally {
            responseBuffers.close();
        }
    }

    private static void receiveReplyAsync(final InternalConnection internalConnection, final SimpleCommandMessage message,
                                          final SingleResultCallback<ReplyMessage<BsonDocument>> callback) {
        internalConnection.receiveMessageAsync(message.getId(),
                                               new SingleResultCallback<ResponseBuffers>() {
                                                   @Override
                                                   public void onResult(final ResponseBuffers result, final Throwable t) {
                                                       try {
                                                           if (t != null) {
                                                               callback.onResult(null, t);
                                                           } else {
                                                               callback.onResult(new ReplyMessage<BsonDocument>(result,
                                                                                                                new BsonDocumentCodec(),
                                                                                                                message.getId()), null);
                                                           }
                                                       } finally {
                                                           if (result != null) {
                                                               result.close();
                                                           }
                                                       }
                                                   }
                                               });
    }

    private static MongoCommandException createCommandFailureException(final BsonDocument reply,
                                                                       final InternalConnection internalConnection) {
        return new MongoCommandException(reply, internalConnection.getDescription().getServerAddress());
    }

    private CommandHelper() {
    }
}

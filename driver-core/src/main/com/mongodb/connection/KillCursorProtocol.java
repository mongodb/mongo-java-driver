/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

import java.util.List;

import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.lang.String.format;

/**
 * An implementation of the OP_KILL_CURSOR protocol.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors OP_KILL_CURSOR
 */
class KillCursorProtocol implements Protocol<Void> {
    public static final Logger LOGGER = Loggers.getLogger("protocol.killcursor");
    private static final String COMMAND_NAME = "killCursors";

    private final MongoNamespace namespace;
    private final List<Long> cursors;
    private CommandListener commandListener;

    KillCursorProtocol(final MongoNamespace namespace, final List<Long> cursors) {
        this.namespace = namespace;
        this.cursors = cursors;
    }

    @Override
    public Void execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(),
                                connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        }
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        long startTimeNanos = System.nanoTime();
        KillCursorsMessage message = null;
        try {
            message = new KillCursorsMessage(cursors);
            if (commandListener != null && namespace != null) {
                sendCommandStartedEvent(message, namespace.getDatabaseName(), COMMAND_NAME, asCommandDocument(),
                                        connection.getDescription(), commandListener);
            }
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            if (commandListener != null && namespace != null) {
                sendCommandSucceededEvent(message, COMMAND_NAME, asCommandResponseDocument(),
                                          connection.getDescription(),
                                          startTimeNanos, commandListener);
            }
            return null;
        } catch (RuntimeException e) {
            if (commandListener != null && namespace != null) {
                sendCommandFailedEvent(message, COMMAND_NAME, connection.getDescription(), startTimeNanos, e, commandListener);
            }
            throw e;
        }
        finally {
            bsonOutput.close();
        }
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<Void> callback) {
        final long startTimeNanos = System.nanoTime();
        final KillCursorsMessage message = new KillCursorsMessage(cursors);
        boolean startEventSent = false;
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(),
                                    connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
            }
            final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);

            if (commandListener != null && namespace != null) {
                sendCommandStartedEvent(message, namespace.getDatabaseName(), COMMAND_NAME, asCommandDocument(),
                        connection.getDescription(), commandListener);
                startEventSent = true;
            }

            message.encode(bsonOutput);
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (commandListener != null && namespace != null) {
                        if (t != null) {
                            sendCommandFailedEvent(message, COMMAND_NAME, connection.getDescription(), startTimeNanos, t, commandListener);
                        } else {
                            sendCommandSucceededEvent(message, COMMAND_NAME, asCommandResponseDocument(),
                                    connection.getDescription(),
                                    startTimeNanos, commandListener);
                        }
                    }

                    bsonOutput.close();
                    callback.onResult(result, t);
                }
            });
        } catch (Throwable t) {
            if (startEventSent) {
                sendCommandFailedEvent(message, COMMAND_NAME, connection.getDescription(), startTimeNanos, t, commandListener);
            }
            callback.onResult(null, t);
        }
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
    }

    private BsonDocument asCommandDocument() {
        BsonArray array = new BsonArray();
        for (long cursor : cursors) {
            array.add(new BsonInt64(cursor));
        }
        return new BsonDocument(COMMAND_NAME, namespace == null ? new BsonInt32(1) : new BsonString(namespace.getCollectionName()))
               .append("cursors", array);
    }

    private BsonDocument asCommandResponseDocument() {
        BsonArray cursorIdArray = new BsonArray();
        for (long cursorId : cursors) {
            cursorIdArray.add(new BsonInt64(cursorId));
        }
        return new BsonDocument("ok", new BsonDouble(1))
               .append("cursorsUnknown", cursorIdArray);

    }

    private String getCursorIdListAsString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < cursors.size(); i++) {
            Long cursor = cursors.get(i);
            builder.append(cursor);
            if (i < cursors.size() - 1) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }
}

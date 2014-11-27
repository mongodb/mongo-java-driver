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

import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.List;

import static java.lang.String.format;

/**
 * An implementation of the OP_KILL_CURSOR protocol.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors OP_KILL_CURSOR
 */
class KillCursorProtocol implements Protocol<Void> {
    public static final Logger LOGGER = Loggers.getLogger("protocol.killcursor");

    private final List<Long> cursors;

    /**
     * Construct an instance.
     *
     * @param cursors the list of cursors to kill
     */
    public KillCursorProtocol(final List<Long> cursors) {
        this.cursors = cursors;
    }

    @Override
    public Void execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(),
                                connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
        }
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            KillCursorsMessage message = new KillCursorsMessage(cursors);
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return null;
        } finally {
            bsonOutput.close();
        }
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<Void> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(),
                                    connection.getDescription().getConnectionId(), connection.getDescription().getServerAddress()));
            }
            final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            KillCursorsMessage message = new KillCursorsMessage(cursors);
            message.encode(bsonOutput);
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    bsonOutput.close();
                    callback.onResult(result, t);
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
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

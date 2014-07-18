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

import com.mongodb.MongoException;
import com.mongodb.connection.ByteBufferOutputBuffer;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import org.mongodb.MongoFuture;
import org.mongodb.ServerCursor;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.protocol.message.KillCursorsMessage;

import static java.lang.String.format;

public class KillCursorProtocol implements Protocol<Void> {
    public static final Logger LOGGER = Loggers.getLogger("protocol.killcursor");

    private final KillCursor killCursor;

    public KillCursorProtocol(final KillCursor killCursor) {
        this.killCursor = killCursor;
    }

    @Override
    public Void execute(final Connection connection) {
        LOGGER.debug(format("Killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(), connection.getId(),
                            connection.getServerAddress()));
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            KillCursorsMessage message = new KillCursorsMessage(killCursor);
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return null;
        } finally {
            buffer.close();
        }
    }

    @Override
    public MongoFuture<Void> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(),
                            connection.getId(),
                            connection.getServerAddress()));
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
        final ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        KillCursorsMessage message = new KillCursorsMessage(killCursor);
        message.encode(buffer);
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                buffer.close();
                retVal.init(result, e);
            }
        });
        return retVal;
    }

    private String getCursorIdListAsString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < killCursor.getServerCursors().size(); i++) {
            ServerCursor cursor = killCursor.getServerCursors().get(i);
            builder.append(cursor.getId());
            if (i < killCursor.getServerCursors().size() - 1) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }
}

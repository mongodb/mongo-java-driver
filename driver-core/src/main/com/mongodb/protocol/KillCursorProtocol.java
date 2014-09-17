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

import com.mongodb.MongoException;
import com.mongodb.ServerCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.Connection;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.protocol.message.KillCursorsMessage;

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
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            KillCursorsMessage message = new KillCursorsMessage(killCursor);
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return null;
        } finally {
            bsonOutput.close();
        }
    }

    @Override
    public MongoFuture<Void> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously killing cursors [%s] on connection [%s] to server %s", getCursorIdListAsString(),
                            connection.getId(),
                            connection.getServerAddress()));
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
        final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        KillCursorsMessage message = new KillCursorsMessage(killCursor);
        message.encode(bsonOutput);
        connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                bsonOutput.close();
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

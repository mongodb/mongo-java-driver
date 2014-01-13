/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.protocol.message.KillCursorsMessage;

import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public class KillCursorProtocol implements Protocol<Void> {
    private final KillCursor killCursor;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;
    private final BufferProvider bufferProvider;

    public KillCursorProtocol(final KillCursor killCursor, final BufferProvider bufferProvider,
                              final ServerDescription serverDescription, final Connection connection,
                              final boolean closeConnection) {
        this.bufferProvider = bufferProvider;
        this.killCursor = killCursor;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    @Override
    public Void execute() {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            KillCursorsMessage message = new KillCursorsMessage(killCursor, getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return null;
        } finally {
            buffer.close();
            if (closeConnection) {
                connection.close();
            }
        }
    }

    @Override
    public MongoFuture<Void> executeAsync() {
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        KillCursorsMessage message = new KillCursorsMessage(killCursor, getMessageSettings(serverDescription));
        message.encode(buffer);
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                buffer.close();
                if (closeConnection) {
                    connection.close();
                }
                retVal.init(result, e);
            }
        });
        return retVal;
    }
}

/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation.protocol;

import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.KillCursor;

import static org.mongodb.operation.OperationHelpers.getMessageSettings;

public class KillCursorProtocolOperation implements ProtocolOperation<Void> {
    private final KillCursor killCursor;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private boolean closeConnection;
    private final BufferProvider bufferProvider;

    public KillCursorProtocolOperation(final KillCursor killCursor, final BufferProvider bufferProvider,
                                       final ServerDescription serverDescription, final Connection connection,
                                       final boolean closeConnection) {
        this.bufferProvider = bufferProvider;
        this.killCursor = killCursor;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    public Void execute() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final KillCursorsMessage message = new KillCursorsMessage(killCursor, getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return null;
        } finally {
            buffer.close();
            if (closeConnection) {
                connection.close();
            }
        }
    }
}

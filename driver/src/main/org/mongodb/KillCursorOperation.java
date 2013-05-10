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

package org.mongodb;

import org.mongodb.impl.MongoSyncConnection;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.protocol.MongoKillCursorsMessage;

import java.nio.ByteBuffer;

public class KillCursorOperation extends Operation {
    private final MongoKillCursor killCursor;

    public KillCursorOperation(final MongoKillCursor killCursor, final BufferPool<ByteBuffer> bufferPool) {
        super(null, bufferPool);
        this.killCursor = killCursor;
    }

    public void execute(final MongoServerBinding binding) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(killCursor.getServerCursor().getAddress());
        MongoSyncConnection connection = connectionManager.getConnection();
        try {
            execute(connection);
        } finally {
            connection.close();
        }

    }

    public void execute(final MongoSyncConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            final MongoKillCursorsMessage message = new MongoKillCursorsMessage(killCursor);
            message.encode(buffer);
            connection.sendMessage(buffer);
        } finally {
            buffer.close();
        }
    }

    public MongoKillCursor getKillCursor() {
        return killCursor;
    }
}

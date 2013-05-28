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

package org.mongodb.operation;

import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerConnection;
import org.mongodb.operation.protocol.KillCursorsMessage;
import org.mongodb.session.Session;

import java.nio.ByteBuffer;

public class KillCursorOperation extends Operation {
    private final MongoKillCursor killCursor;

    public KillCursorOperation(final MongoKillCursor killCursor, final BufferPool<ByteBuffer> bufferPool) {
        super(null, bufferPool);
        this.killCursor = killCursor;
    }

    public void execute(final Session session) {
        ServerConnection connection = session.getConnection();
        try {
            execute(connection);
        } finally {
            connection.close();
        }

    }

    public void execute(final ServerConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            final KillCursorsMessage message = new KillCursorsMessage(killCursor,
                    getMessageSettings(connection.getDescription()));
            message.encode(buffer);
            connection.sendMessage(buffer);
        } finally {
            buffer.close();
        }
    }
}

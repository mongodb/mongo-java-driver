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

import org.mongodb.command.MongoCommand;
import org.mongodb.impl.MongoSyncConnection;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.protocol.MongoCommandMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.result.CommandResult;
import org.mongodb.util.Session;

import java.nio.ByteBuffer;

public class CommandOperation extends Operation {
    private final MongoCommand commandOperation;
    private final Codec<Document> codec;

    public CommandOperation(final String database, final MongoCommand commandOperation, final Codec<Document> codec,
                            final BufferPool<ByteBuffer> bufferPool) {
        super(new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME), bufferPool);
        this.commandOperation = commandOperation;
        this.codec = codec;
    }

    public CommandResult execute(final Session session) {
        MongoSyncConnection connection = session.getConnection(commandOperation.getReadPreference());
        try {
            return execute(connection);
        } finally {
            connection.close();
        }
    }

    public CommandResult execute(final MongoSyncConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            final MongoCommandMessage message = new MongoCommandMessage(getNamespace().getFullName(), commandOperation, codec);
            message.encode(buffer);
            final ResponseBuffers responseBuffers = connection.sendAndReceiveMessage(buffer);
            try {
                MongoReplyMessage<Document> replyMessage = new MongoReplyMessage<Document>(responseBuffers, codec);
                return createCommandResult(commandOperation, replyMessage, connection);
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }
}

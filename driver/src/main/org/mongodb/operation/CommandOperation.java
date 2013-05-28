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

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.command.MongoCommand;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.operation.protocol.MongoCommandMessage;
import org.mongodb.operation.protocol.MongoReplyMessage;
import org.mongodb.session.ServerSelectingSession;

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

    public CommandResult execute(final ServerSelectingSession session) {
        ServerConnection connection = session.getConnection(new ReadPreferenceServerSelector(commandOperation.getReadPreference()));
        try {
            return execute(connection);
        } finally {
            connection.close();
        }
    }

    public CommandResult execute(final ServerConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            final MongoCommandMessage message = new MongoCommandMessage(getNamespace().getFullName(), commandOperation, codec);
            message.encode(buffer, connection.getDescription());
            connection.sendMessage(buffer);
            final ResponseBuffers responseBuffers = connection.receiveMessage();
            try {
                MongoReplyMessage<Document> replyMessage = new MongoReplyMessage<Document>(responseBuffers, codec, message.getId());
                return createCommandResult(commandOperation, replyMessage, connection);
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }
}

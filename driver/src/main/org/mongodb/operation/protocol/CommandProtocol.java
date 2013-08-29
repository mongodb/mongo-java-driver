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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;

import static org.mongodb.operation.OperationHelpers.createCommandResult;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class CommandProtocol implements Protocol<CommandResult> {
    private final MongoNamespace namespace;
    private final Document command;
    private final Decoder<Document> commandEncoder;
    private final Encoder<Document> commandResultDecoder;
    private final BufferProvider bufferProvider;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    public CommandProtocol(final String database, final Document command, final Encoder<Document> commandEncoder,
                           final Decoder<Document> commandResultDecoder, final BufferProvider bufferProvider,
                           final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = command;
        this.commandEncoder = commandResultDecoder;
        this.commandResultDecoder = commandEncoder;
        this.bufferProvider = bufferProvider;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    public CommandResult execute() {
        try {
            final CommandMessage sentMessage = sendMessage();
            return receiveMessage(sentMessage.getId());
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    private CommandMessage sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final CommandMessage message = new CommandMessage(namespace.getFullName(), command, commandResultDecoder,
                                                              getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final int messageId) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(getResponseSettings(serverDescription, messageId));
        try {
            final ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, commandEncoder, messageId);
            return createCommandResult(replyMessage, connection);
        } finally {
            responseBuffers.close();
        }
    }

}

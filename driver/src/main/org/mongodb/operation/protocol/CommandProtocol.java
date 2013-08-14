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

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.command.Command;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;

import static org.mongodb.operation.OperationHelpers.createCommandResult;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class CommandProtocol implements Protocol<CommandResult> {
    private final Command command;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final Encoder<Document> encoder;
    private final Decoder<Document> decoder;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    public CommandProtocol(final String database, final Command command, final Codec<Document> codec,
                           final BufferProvider bufferProvider, final ServerDescription serverDescription,
                           final Connection connection, final boolean closeConnection) {

        this(database, command, codec, codec, bufferProvider, serverDescription, connection, closeConnection);
    }

    public CommandProtocol(final String database, final Command command, final Encoder<Document> encoder, final Decoder<Document> decoder,
                           final BufferProvider bufferProvider, final ServerDescription serverDescription,
                           final Connection connection, final boolean closeConnection) {
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.bufferProvider = bufferProvider;
        this.command = command;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    public Command getCommand() {
        return command;
    }

    public CommandResult execute() {
        try {
            return receiveMessage(sendMessage());
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    private CommandMessage sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final CommandMessage message = new CommandMessage(namespace.getFullName(), command, encoder,
                    getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final CommandMessage message) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(serverDescription, message.getId()));
        try {
            ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, decoder, message.getId());
            return createCommandResult(command, replyMessage, connection);
        } finally {
            responseBuffers.close();
        }
    }
}

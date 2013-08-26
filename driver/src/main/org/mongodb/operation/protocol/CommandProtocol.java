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
import org.mongodb.connection.Channel;
import org.mongodb.connection.ChannelReceiveArgs;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;

import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.operation.OperationHelpers.createCommandResult;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;

public class CommandProtocol implements Protocol<CommandResult> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final MongoNamespace namespace;
    private final Document command;
    private final Decoder<Document> commandEncoder;
    private final Encoder<Document> commandResultDecoder;
    private final BufferProvider bufferProvider;
    private final ServerDescription serverDescription;
    private final Channel channel;
    private final boolean closeChannel;

    public CommandProtocol(final String database, final Document command, final Encoder<Document> commandEncoder,
                           final Decoder<Document> commandResultDecoder, final BufferProvider bufferProvider,
                           final ServerDescription serverDescription, final Channel channel, final boolean closeChannel) {
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = command;
        this.commandEncoder = commandResultDecoder;
        this.commandResultDecoder = commandEncoder;
        this.bufferProvider = bufferProvider;
        this.serverDescription = serverDescription;
        this.channel = channel;
        this.closeChannel = closeChannel;
    }

    public CommandResult execute() {
        try {
            LOGGER.fine(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                    command.keySet().iterator().next(), command.values().iterator().next(),
                    namespace.getDatabaseName(), channel.getId(), channel.getServerAddress()));
            CommandResult commandResult = receiveMessage(sendMessage().getId());
            LOGGER.fine("Command execution complete");
            return commandResult;
        } finally {
            if (closeChannel) {
                channel.close();
            }
        }
    }

    private CommandMessage sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final CommandMessage message = new CommandMessage(namespace.getFullName(), command, commandResultDecoder,
                                                              getMessageSettings(serverDescription));
            message.encode(buffer);
            channel.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final int messageId) {
        final ResponseBuffers responseBuffers = channel.receiveMessage(new ChannelReceiveArgs(messageId));
        try {
            final ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, commandEncoder, messageId);
            return createCommandResult(replyMessage, channel.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

}

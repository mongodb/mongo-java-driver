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
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.GetLastError;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;

import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static org.mongodb.command.GetLastError.parseGetLastErrorResponse;
import static org.mongodb.operation.OperationHelpers.createCommandResult;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public abstract class WriteProtocol implements Protocol<CommandResult> {

    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;
    private final GetLastError getLastErrorCommand;

    public WriteProtocol(final MongoNamespace namespace, final BufferProvider bufferProvider, final WriteConcern writeConcern,
                         final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;

        if (writeConcern.isAcknowledged()) {
            getLastErrorCommand = new GetLastError(writeConcern);
        }
        else {
            getLastErrorCommand = null;
        }
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
            RequestMessage nextMessage = createRequestMessage(getMessageSettings(serverDescription)).encode(buffer);
            while (nextMessage != null) {
                nextMessage = nextMessage.encode(buffer);
            }
            CommandMessage getLastErrorMessage = null;
            if (getLastErrorCommand != null) {
                getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                            COMMAND_COLLECTION_NAME).getFullName(),
                                                         getLastErrorCommand.toDocument(), new DocumentCodec(),
                                                         getMessageSettings(serverDescription)
                );
                getLastErrorMessage.encode(buffer);
            }
            connection.sendMessage(buffer.getByteBuffers());
            return getLastErrorMessage;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final RequestMessage requestMessage) {
        if (requestMessage == null) {
            return null;
        }
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(serverDescription, requestMessage.getId()));
        try {
            return parseGetLastErrorResponse(createCommandResult(
                                                                new ReplyMessage<Document>(responseBuffers,
                                                                                            new DocumentCodec(),
                                                                                            requestMessage.getId()),
                                                                 connection));
        } finally {
            responseBuffers.close();
        }
    }

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);

    protected MongoNamespace getNamespace() {
        return namespace;
    }
}

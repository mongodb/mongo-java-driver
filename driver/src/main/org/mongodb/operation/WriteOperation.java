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

import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerSelectingOperation;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.GetLastError;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.protocol.CommandMessage;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.ReplyMessage;
import org.mongodb.operation.protocol.RequestMessage;
import org.mongodb.session.PrimaryServerSelector;

import static org.mongodb.operation.OperationHelpers.createCommandResult;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public abstract class WriteOperation implements ServerSelectingOperation<CommandResult> {

    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public WriteOperation(final MongoNamespace namespace, final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public CommandResult execute(final ServerConnection connection) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            CommandResult getLastErrorResult = null;
            RequestMessage nextMessage = createRequestMessage(getMessageSettings(connection.getDescription())).encode(buffer);
            while (nextMessage != null) {
                nextMessage = nextMessage.encode(buffer);
            }
            if (getWrite().getWriteConcern().callGetLastError()) {
                final GetLastError getLastError = new GetLastError(getWrite().getWriteConcern());
                final DocumentCodec codec = new DocumentCodec();
                CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                        MongoNamespace.COMMAND_COLLECTION_NAME).getFullName(), getLastError,
                        codec, getMessageSettings(connection.getDescription()));
                getLastErrorMessage.encode(buffer);
                connection.sendMessage(buffer.getByteBuffers());
                ResponseBuffers responseBuffers = connection.receiveMessage(
                        getResponseSettings(connection.getDescription(), getLastErrorMessage.getId()));
                try {
                    getLastErrorResult = getLastError.parseGetLastErrorResponse(createCommandResult(getLastError,
                            new ReplyMessage<Document>(responseBuffers, codec, getLastErrorMessage.getId()), connection));
                } finally {
                    responseBuffers.close();
                }
            }
            else {
                connection.sendMessage(buffer.getByteBuffers());
            }
            return getLastErrorResult;
        } finally {
            buffer.close();
        }
    }

    @Override
    public ServerSelector getServerSelector() {
        return new PrimaryServerSelector();
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    protected abstract BaseWrite getWrite();

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);

    protected MongoNamespace getNamespace() {
        return namespace;
    }
}

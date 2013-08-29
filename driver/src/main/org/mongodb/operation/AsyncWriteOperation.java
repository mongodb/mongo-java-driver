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

import org.mongodb.AsyncServerSelectingOperation;
import org.mongodb.CommandResult;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.GetLastError;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.protocol.CommandMessage;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.RequestMessage;
import org.mongodb.session.PrimaryServerSelector;

import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static org.mongodb.operation.OperationHelpers.encodeMessageToBuffer;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;

public abstract class AsyncWriteOperation implements AsyncServerSelectingOperation<CommandResult> {
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public AsyncWriteOperation(final MongoNamespace namespace, final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
    }

    public MongoFuture<CommandResult> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        final RequestMessage nextMessage = encodeMessageToBuffer(createRequestMessage(getMessageSettings(connection.getDescription())),
                buffer);
        if (getWriteConcern().isAcknowledged()) {
            final GetLastError getLastError = new GetLastError(getWriteConcern());
            final CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                                             COMMAND_COLLECTION_NAME).getFullName(),
                                                                          getLastError.toDocument(),
                                                                          new DocumentCodec(),
                                                                          getMessageSettings(connection.getDescription()));
            encodeMessageToBuffer(getLastErrorMessage, buffer);
            connection.sendMessage(buffer.getByteBuffers(),
                    new SendMessageCallback<CommandResult>(connection, buffer, getLastErrorMessage.getId(), retVal,
                            new WriteResultCallback(retVal, getWrite(), new DocumentCodec(),
                                    getNamespace(), nextMessage, connection, bufferProvider, getLastErrorMessage.getId())));
        }
        else {
            connection.sendMessage(buffer.getByteBuffers(), new UnacknowledgedWriteResultCallback(retVal, getWrite(),
                    getNamespace(), nextMessage, connection, buffer, bufferProvider));
        }
        return retVal;
    }

    @Override
    public ServerSelector getServerSelector() {
        return new PrimaryServerSelector();
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);

    public abstract BaseWrite getWrite();

    public abstract WriteConcern getWriteConcern();

    protected MongoNamespace getNamespace() {
        return namespace;
    }
}

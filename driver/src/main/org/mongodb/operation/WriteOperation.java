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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.GetLastError;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.operation.protocol.CommandMessage;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.ReplyMessage;
import org.mongodb.operation.protocol.RequestMessage;
import org.mongodb.session.Session;

public abstract class WriteOperation extends Operation {

    public WriteOperation(final MongoNamespace namespace, final BufferProvider bufferProvider) {
        super(namespace, bufferProvider);
    }

    public CommandResult execute(final Session session) {
        ServerConnection connection = session.getConnection();
        try {
            return execute(connection);
        } finally {
            connection.close();
        }
    }

    public CommandResult execute(final ServerConnection connection) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
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

    public abstract BaseWrite getWrite();

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);
}

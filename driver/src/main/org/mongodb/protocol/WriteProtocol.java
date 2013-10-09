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

package org.mongodb.protocol;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.ReplyMessage;
import org.mongodb.protocol.message.RequestMessage;

import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public abstract class WriteProtocol implements Protocol<WriteResult> {

    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final WriteConcern writeConcern;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    public WriteProtocol(final MongoNamespace namespace, final BufferProvider bufferProvider, final WriteConcern writeConcern,
                         final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.writeConcern = writeConcern;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    public WriteResult execute() {
        try {
            return receiveMessage(sendMessage());
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    public MongoFuture<WriteResult> executeAsync() {
        SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        RequestMessage requestMessage = createRequestMessage(getMessageSettings(serverDescription));
        RequestMessage nextMessage = encodeMessageToBuffer(requestMessage, buffer);
        if (writeConcern.isAcknowledged()) {
            CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                                       COMMAND_COLLECTION_NAME).getFullName(),
                                                                    writeConcern.asDocument(),
                                                                    new DocumentCodec(),
                                                                    getMessageSettings(serverDescription));
            encodeMessageToBuffer(getLastErrorMessage, buffer);
            connection.sendMessageAsync(buffer.getByteBuffers(), getLastErrorMessage.getId(),
                                        new SendMessageCallback<WriteResult>(connection, buffer, getLastErrorMessage.getId(), retVal,
                                                                             new WriteResultCallback(retVal,
                                                                                                     new DocumentCodec(),
                                                                                                     getNamespace(),
                                                                                                     nextMessage,
                                                                                                     writeConcern,
                                                                                                     getLastErrorMessage.getId(),
                                                                                                     bufferProvider,
                                                                                                     serverDescription,
                                                                                                     connection,
                                                                                                     closeConnection)));
        } else {
            connection.sendMessageAsync(buffer.getByteBuffers(), requestMessage.getId(),
                                        new UnacknowledgedWriteResultCallback(retVal,
                                                                              getNamespace(),
                                                                              nextMessage,
                                                                              buffer,
                                                                              bufferProvider,
                                                                              serverDescription,
                                                                              connection,
                                                                              closeConnection));
        }
        return retVal;
    }


    private CommandMessage sendMessage() {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            RequestMessage lastMessage = createRequestMessage(getMessageSettings(serverDescription));
            RequestMessage nextMessage = lastMessage.encode(buffer);
            int batchNum = 1;
            if (nextMessage != null) {
                getLogger().fine(format("Sending batch %d", batchNum));
            }

            while (nextMessage != null) {
                batchNum++;
                getLogger().fine(format("Sending batch %d", batchNum));
                lastMessage = nextMessage;
                nextMessage = nextMessage.encode(buffer);
            }
            CommandMessage getLastErrorMessage = null;
            if (writeConcern.isAcknowledged()) {
                getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                            COMMAND_COLLECTION_NAME).getFullName(),
                                                         writeConcern.asDocument(), new DocumentCodec(),
                                                         getMessageSettings(serverDescription)
                );
                getLastErrorMessage.encode(buffer);
                lastMessage = getLastErrorMessage;
            }
            connection.sendMessage(buffer.getByteBuffers(), lastMessage.getId());
            return getLastErrorMessage;
        } finally {
            buffer.close();
        }
    }

    private WriteResult receiveMessage(final RequestMessage requestMessage) {
        if (requestMessage == null) {
            return null;
        }
        ResponseBuffers responseBuffers = connection.receiveMessage(requestMessage.getId());
        try {
            ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), requestMessage.getId());
            WriteResult result = new WriteResult(new CommandResult(connection.getServerAddress(),
                                                                   replyMessage.getDocuments().get(0),
                                                                   replyMessage.getElapsedNanoseconds()), writeConcern);
            throwIfWriteException(result);
            return result;
        } finally {
            responseBuffers.close();
        }
    }

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);

    protected MongoNamespace getNamespace() {
        return namespace;
    }

    protected Connection getConnection() {
        return connection;
    }

    protected abstract Logger getLogger();

    private void throwIfWriteException(final WriteResult result) {
        MongoException exception = ProtocolHelper.getWriteException(result);
        if (exception != null) {
            throw exception;
        }
    }
}

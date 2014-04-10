/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ByteBufferOutputBuffer;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.ReplyMessage;
import org.mongodb.protocol.message.RequestMessage;

import java.util.EnumSet;

import static java.lang.String.format;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public abstract class WriteProtocol implements Protocol<WriteResult> {

    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;

    public WriteProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
    }

    public WriteResult execute(final Connection connection, final ServerDescription serverDescription) {
        return receiveMessage(connection, sendMessage(connection, serverDescription));
    }

    public MongoFuture<WriteResult> executeAsync(final Connection connection, final ServerDescription serverDescription) {
        SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        RequestMessage requestMessage = createRequestMessage(getMessageSettings(serverDescription));
        RequestMessage nextMessage = encodeMessageToBuffer(requestMessage, buffer);
        if (writeConcern.isAcknowledged()) {
            CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                                       COMMAND_COLLECTION_NAME).getFullName(),
                                                                    createGetLastErrorCommandDocument(),
                                                                    EnumSet.noneOf(QueryFlag.class), new DocumentCodec(),
                                                                    getMessageSettings(serverDescription));
            encodeMessageToBuffer(getLastErrorMessage, buffer);
            connection.sendMessageAsync(buffer.getByteBuffers(), getLastErrorMessage.getId(),
                                             new SendMessageCallback<WriteResult>(connection,
                                                                                  buffer,
                                                                                  getLastErrorMessage.getId(),
                                                                                  retVal,
                                                                                  new WriteResultCallback(retVal,
                                                                                                          new DocumentCodec(),
                                                                                                          getNamespace(),
                                                                                                          nextMessage,
                                                                                                          ordered,
                                                                                                          writeConcern,
                                                                                                          getLastErrorMessage.getId(),
                                                                                                          connection, serverDescription)));
        } else {
            connection.sendMessageAsync(buffer.getByteBuffers(), requestMessage.getId(),
                                             new UnacknowledgedWriteResultCallback(retVal, getNamespace(), nextMessage, ordered, buffer,
                                                                                   connection, serverDescription));
        }
        return retVal;
    }


    private CommandMessage sendMessage(final Connection connection, final ServerDescription serverDescription) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            RequestMessage lastMessage = createRequestMessage(getMessageSettings(serverDescription));
            RequestMessage nextMessage = lastMessage.encode(buffer);
            int batchNum = 1;
            if (nextMessage != null) {
                getLogger().debug(format("Sending batch %d", batchNum));
            }

            while (nextMessage != null) {
                batchNum++;
                getLogger().debug(format("Sending batch %d", batchNum));
                lastMessage = nextMessage;
                nextMessage = nextMessage.encode(buffer);
            }
            CommandMessage getLastErrorMessage = null;
            if (writeConcern.isAcknowledged()) {
                getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                            COMMAND_COLLECTION_NAME).getFullName(),
                                                         createGetLastErrorCommandDocument(), EnumSet.noneOf(QueryFlag.class),
                                                         new DocumentCodec(), getMessageSettings(serverDescription)
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

    private Document createGetLastErrorCommandDocument() {
        Document command = new Document("getlasterror", 1);
        command.putAll(writeConcern.asDocument());
        return command;
    }

    private WriteResult receiveMessage(final Connection connection, final RequestMessage requestMessage) {
        if (requestMessage == null) {
            return new UnacknowledgedWriteResult();
        }
        ResponseBuffers responseBuffers = connection.receiveMessage(requestMessage.getId());
        try {
            ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), requestMessage.getId());
            return ProtocolHelper.getWriteResult(new CommandResult(connection.getServerAddress(), replyMessage.getDocuments().get(0),
                                                                   replyMessage.getElapsedNanoseconds()));
        } finally {
            responseBuffers.close();
        }
    }

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);

    protected MongoNamespace getNamespace() {
        return namespace;
    }

    protected boolean isOrdered() {
        return ordered;
    }

    protected WriteConcern getWriteConcern() {
        return writeConcern;
    }

    protected abstract Logger getLogger();
}

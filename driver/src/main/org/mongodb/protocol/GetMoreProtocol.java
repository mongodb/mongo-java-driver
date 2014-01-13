/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.GetMoreMessage;
import org.mongodb.protocol.message.ReplyMessage;

import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static org.mongodb.protocol.ProtocolHelper.getQueryFailureException;

public class GetMoreProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.getmore");

    private final GetMore getMore;
    private final Decoder<T> resultDecoder;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public GetMoreProtocol(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                           final BufferProvider bufferProvider, final ServerDescription serverDescription, final Connection connection,
                           final boolean closeConnection) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    @Override
    public QueryResult<T> execute() {
        try {
            LOGGER.fine(format("Getting more documents from cursor with id %d on connection [%s] to server %s",
                               getMore.getServerCursor().getId(), connection.getId(), connection.getServerAddress()));
            QueryResult<T> queryResult = receiveMessage(sendMessage());
            LOGGER.fine("Get-more completed");
            return queryResult;
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    public MongoFuture<QueryResult<T>> executeAsync() {
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore, getMessageSettings(serverDescription));
        encodeMessageToBuffer(message, buffer);
        GetMoreResultCallback<T> receiveCallback = new GetMoreResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                                resultDecoder,
                                                                                getMore.getServerCursor()
                                                                                       .getId(),
                                                                                message.getId(),
                                                                                connection,
                                                                                closeConnection);
        connection.sendMessageAsync(buffer.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<QueryResult<T>>(connection, buffer, message.getId(), retVal, receiveCallback));
        return retVal;
    }


    private GetMoreMessage sendMessage() {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore, getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return message;
        } finally {
            buffer.close();
        }
    }

    private QueryResult<T> receiveMessage(final GetMoreMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(message.getCursorId(), connection.getServerAddress()));
            }

            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                Document errorDocument = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(),
                                                                    message.getId()).getDocuments().get(0);
                throw getQueryFailureException(connection.getServerAddress(), errorDocument);
            }

            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId()),
                                      connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}

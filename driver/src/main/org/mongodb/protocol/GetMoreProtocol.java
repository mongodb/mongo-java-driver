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

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import org.bson.codecs.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.ByteBufferOutputBuffer;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.GetMoreMessage;
import org.mongodb.protocol.message.ReplyMessage;

import static java.lang.String.format;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getQueryFailureException;

public class GetMoreProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.getmore");

    private final GetMore getMore;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;

    public GetMoreProtocol(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public QueryResult<T> execute(final Connection connection) {
        LOGGER.debug(format("Getting more documents from namespace %s with cursor %d on connection [%s] to server %s",
                            namespace, getMore.getServerCursor().getId(), connection.getId(), connection.getServerAddress()));
        QueryResult<T> queryResult = receiveMessage(connection, sendMessage(connection));
        LOGGER.debug("Get-more completed");
        return queryResult;
    }

    public MongoFuture<QueryResult<T>> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously getting more documents from namespace %s with cursor %d on connection [%s] to server %s",
                            namespace, getMore.getServerCursor().getId(), connection.getId(), connection.getServerAddress()));
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore);
        encodeMessageToBuffer(message, buffer);
        GetMoreResultCallback<T> receiveCallback = new GetMoreResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                                resultDecoder,
                                                                                getMore.getServerCursor().getId(),
                                                                                message.getId(),
                                                                                connection.getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(),
                                         message.getId(),
                                         new SendMessageCallback<QueryResult<T>>(connection, buffer, message.getId(), retVal,
                                                                                 receiveCallback));
        return retVal;
    }


    private GetMoreMessage sendMessage(final Connection connection) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore);
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return message;
        } finally {
            buffer.close();
        }
    }

    private QueryResult<T> receiveMessage(final Connection connection, final GetMoreMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                throw new MongoCursorNotFoundException(message.getCursorId(), connection.getServerAddress());
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

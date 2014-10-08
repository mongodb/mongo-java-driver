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

package com.mongodb.protocol;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.operation.GetMore;
import com.mongodb.protocol.message.GetMoreMessage;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.codecs.Decoder;
import org.mongodb.Document;

import static com.mongodb.protocol.ProtocolHelper.encodeMessage;
import static com.mongodb.protocol.ProtocolHelper.getQueryFailureException;
import static java.lang.String.format;

/**
 * An implementation of the OP_GET_MORE protocol.
 *
 * @param <T> the type of document to decode query results to
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 * @since 3.0
 */
public class GetMoreProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.getmore");

    private final GetMore getMore;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;

    /**
     * Construct an instance.
     *
     * @param namespace     the namespace
     * @param getMore       the values to apply to the OP_GET_MORE
     * @param resultDecoder the decoder for the result documents.
     */
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

    @Override
    public MongoFuture<QueryResult<T>> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously getting more documents from namespace %s with cursor %d on connection [%s] to server %s",
                            namespace, getMore.getServerCursor().getId(), connection.getId(), connection.getServerAddress()));
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore);
        encodeMessage(message, bsonOutput);
        GetMoreResultCallback<T> receiveCallback = new GetMoreResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                                resultDecoder,
                                                                                getMore.getServerCursor().getId(),
                                                                                message.getId(),
                                                                                connection.getServerAddress());
        connection.sendMessageAsync(bsonOutput.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<QueryResult<T>>(connection, bsonOutput, message.getId(), retVal,
                                                                            receiveCallback));
        return retVal;
    }


    private GetMoreMessage sendMessage(final Connection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore);
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
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

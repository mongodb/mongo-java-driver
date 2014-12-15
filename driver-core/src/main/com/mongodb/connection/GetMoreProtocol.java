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

package com.mongodb.connection;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.connection.ProtocolHelper.getQueryFailureException;
import static java.lang.String.format;

/**
 * An implementation of the OP_GET_MORE protocol.
 *
 * @param <T> the type of document to decode query results to
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class GetMoreProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.getmore");

    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;
    private final long cursorId;
    private final int numberToReturn;

    /**
     * Construct an instance.
     *
     * @param namespace      the namespace
     * @param cursorId       the cursor id
     * @param numberToReturn the number of documents to return
     * @param resultDecoder  the decoder for the result documents.
     */
    public GetMoreProtocol(final MongoNamespace namespace, final long cursorId, final int numberToReturn, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.cursorId = cursorId;
        this.numberToReturn = numberToReturn;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public QueryResult<T> execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Getting more documents from namespace %s with cursor %d on connection [%s] to server %s",
                                namespace, cursorId, connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        QueryResult<T> queryResult = receiveMessage(connection, sendMessage(connection));
        LOGGER.debug("Get-more completed");
        return queryResult;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<QueryResult<T>> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously getting more documents from namespace %s with cursor %d on connection [%s] to server "
                                    + "%s", namespace, cursorId, connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), cursorId, numberToReturn);
            ProtocolHelper.encodeMessage(message, bsonOutput);
            SingleResultCallback<ResponseBuffers> receiveCallback = new GetMoreResultCallback<T>(namespace,
                                                                                                 callback,
                                                                                                 resultDecoder,
                                                                                                 cursorId,
                                                                                                 message.getId(),
                                                                                                 connection.getDescription()
                                                                                                           .getServerAddress());
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(),
                                        new SendMessageCallback<QueryResult<T>>(connection, bsonOutput, message.getId(), callback,
                                                                                receiveCallback));
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private GetMoreMessage sendMessage(final InternalConnection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), cursorId, numberToReturn);
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }

    private QueryResult<T> receiveMessage(final InternalConnection connection, final GetMoreMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                throw new MongoCursorNotFoundException(message.getCursorId(), connection.getDescription().getServerAddress());
            }

            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                BsonDocument errorDocument = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                            message.getId()).getDocuments().get(0);
                throw getQueryFailureException(errorDocument, connection.getDescription().getServerAddress());
            }

            return new QueryResult<T>(namespace, new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId()),
                                      connection.getDescription().getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}

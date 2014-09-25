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

import com.mongodb.CursorFlag;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.protocol.message.QueryMessage;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.mongodb.Document;

import java.util.EnumSet;

import static com.mongodb.protocol.ProtocolHelper.encodeMessage;
import static com.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static com.mongodb.protocol.ProtocolHelper.getQueryFailureException;
import static java.lang.String.format;

/**
 * An implementation of the MongoDB OP_QUERY wire protocol.
 *
 * @param <T> the type of document to decode query results to
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 * @since 3.0
 */
public class QueryProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.query");

    private final EnumSet<CursorFlag> cursorFlags;
    private final int skip;
    private final int numberToReturn;
    private final BsonDocument queryDocument;
    private final BsonDocument fields;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;

    /**
     * Construct an instance.
     *
     * @param namespace      the namespace
     * @param cursorFlags    the cursor flags
     * @param skip           the number of documents to skip
     * @param numberToReturn the number to return
     * @param queryDocument  the query document
     * @param fields         the fields to return in the result documents
     * @param resultDecoder  the decoder for the result documents
     */
    public QueryProtocol(final MongoNamespace namespace, final EnumSet<CursorFlag> cursorFlags, final int skip,
                         final int numberToReturn, final BsonDocument queryDocument,
                         final BsonDocument fields, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.cursorFlags = cursorFlags;
        this.skip = skip;
        this.numberToReturn = numberToReturn;
        this.queryDocument = queryDocument;
        this.fields = fields;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public QueryResult<T> execute(final Connection connection) {
        LOGGER.debug(format("Sending query of namespace %s on connection [%s] to server %s", namespace, connection.getId(),
                            connection.getServerAddress()));
        QueryResult<T> queryResult = receiveMessage(connection, sendMessage(connection));
        LOGGER.debug("Query completed");
        return queryResult;
    }

    @Override
    public MongoFuture<QueryResult<T>> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously sending query of namespace %s on connection [%s] to server %s", namespace, connection.getId(),
                            connection.getServerAddress()));
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        QueryMessage message = createQueryMessage(connection.getDescription());
        encodeMessage(message, bsonOutput);
        QueryResultCallback<T> receiveCallback = new QueryResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                            resultDecoder,
                                                                            message.getId(),
                                                                            connection.getServerAddress());
        connection.sendMessageAsync(bsonOutput.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<QueryResult<T>>(connection, bsonOutput, message.getId(), retVal,
                                                                            receiveCallback));
        return retVal;
    }

    private QueryMessage createQueryMessage(final ConnectionDescription connectionDescription) {
        return new QueryMessage(namespace.getFullName(), cursorFlags, skip, numberToReturn, queryDocument, fields,
                                getMessageSettings(connectionDescription));
    }

    private QueryMessage sendMessage(final Connection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            QueryMessage message = createQueryMessage(connection.getDescription());
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }

    private QueryResult<T> receiveMessage(final Connection connection, final QueryMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                Document errorDocument = new ReplyMessage<Document>(responseBuffers,
                                                                    new DocumentCodec(),
                                                                    message.getId()).getDocuments().get(0);
                throw getQueryFailureException(connection.getServerAddress(), errorDocument);
            }
            ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId());

            return new QueryResult<T>(replyMessage, connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}

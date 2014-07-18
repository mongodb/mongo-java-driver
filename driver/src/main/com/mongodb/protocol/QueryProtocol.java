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

import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.ByteBufferOutputBuffer;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.operation.QueryFlag;
import com.mongodb.operation.SingleResultFuture;
import com.mongodb.operation.SingleResultFutureCallback;
import com.mongodb.protocol.message.QueryMessage;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;

import java.util.EnumSet;

import static com.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static com.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static com.mongodb.protocol.ProtocolHelper.getQueryFailureException;
import static java.lang.String.format;

public class QueryProtocol<T> implements Protocol<QueryResult<T>> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.query");

    private final EnumSet<QueryFlag> queryFlags;
    private final int skip;
    private final int numberToReturn;
    private final BsonDocument queryDocument;
    private final BsonDocument fields;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;

    public QueryProtocol(final MongoNamespace namespace, final EnumSet<QueryFlag> queryFlags, final int skip,
                         final int numberToReturn, final BsonDocument queryDocument,
                         final BsonDocument fields, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.queryFlags = queryFlags;
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

    public MongoFuture<QueryResult<T>> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously sending query of namespace %s on connection [%s] to server %s", namespace, connection.getId(),
                            connection.getServerAddress()));
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        QueryMessage message = createQueryMessage(connection.getServerDescription());
        encodeMessageToBuffer(message, buffer);
        QueryResultCallback<T> receiveCallback = new QueryResultCallback<T>(new SingleResultFutureCallback<QueryResult<T>>(retVal),
                                                                            resultDecoder,
                                                                            message.getId(),
                                                                            connection.getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<QueryResult<T>>(connection, buffer, message.getId(), retVal,
                                                                            receiveCallback)
                                   );
        return retVal;
    }

    private QueryMessage createQueryMessage(final ServerDescription serverDescription) {
        return new QueryMessage(namespace.getFullName(), queryFlags, skip, numberToReturn, queryDocument, fields,
                                getMessageSettings(serverDescription));
    }

    private QueryMessage sendMessage(final Connection connection) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            QueryMessage message = createQueryMessage(connection.getServerDescription());
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return message;
        } finally {
            buffer.close();
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

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

package org.mongodb.operation.protocol;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.Find;
import org.mongodb.operation.MongoQueryFailureException;

import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class QueryProtocol<T> implements Protocol<QueryResult<T>> {
    private final Find find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private boolean closeConnection;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public QueryProtocol(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                         final Decoder<T> resultDecoder, final BufferProvider bufferProvider,
                         final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.resultDecoder = resultDecoder;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    @Override
    public QueryResult<T> execute() {
        try {
            return receiveMessage(sendMessage());
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    private QueryMessage sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            QueryMessage message = new QueryMessage(namespace.getFullName(), find, queryEncoder,
                    getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private QueryResult<T> receiveMessage(final QueryMessage message) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(serverDescription, message.getId()));
        try {
            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                final Document errorDocument =
                        new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId()).getDocuments().get(0);
                throw new MongoQueryFailureException(connection.getServerAddress(), errorDocument);
            }
            final ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId());

            return new QueryResult<T>(replyMessage, connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}

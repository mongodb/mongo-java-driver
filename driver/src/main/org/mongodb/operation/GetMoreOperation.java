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

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.Operation;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.operation.protocol.GetMoreMessage;
import org.mongodb.operation.protocol.ReplyMessage;

import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class GetMoreOperation<T> implements Operation<QueryResult<T>> {
    private final GetMore getMore;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public GetMoreOperation(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                            final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public QueryResult<T> execute(final ServerConnection connection) {
        return receiveMessage(connection, sendMessage(connection));
    }

    private GetMoreMessage sendMessage(final ServerConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore,
                    getMessageSettings(connection.getDescription()));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private QueryResult<T> receiveMessage(final ServerConnection connection, final GetMoreMessage message) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(connection.getDescription(), message.getId()));
        try {
            if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(message.getCursorId(), connection.getServerAddress()));
            }

            if (responseBuffers.getReplyHeader().isQueryFailure()) {
                final Document errorDocument =
                        new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId()).getDocuments().get(0);
                throw new MongoQueryFailureException(connection.getServerAddress(), errorDocument);
            }

            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, message.getId()),
                    connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }
}

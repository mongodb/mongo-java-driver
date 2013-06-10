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
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerSelectingOperation;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.protocol.QueryMessage;
import org.mongodb.operation.protocol.ReplyMessage;

import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class QueryOperation<T> implements ServerSelectingOperation<QueryResult<T>> {
    private final Find find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public QueryOperation(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                          final Decoder<T> resultDecoder, final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public QueryResult<T> execute(final ServerConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final QueryMessage message = new QueryMessage(namespace.getFullName(), find, queryEncoder,
                    getMessageSettings(connection.getDescription()));
            message.encode(buffer);

            connection.sendMessage(buffer.getByteBuffers());
            final ResponseBuffers responseBuffers = connection.receiveMessage(
                    getResponseSettings(connection.getDescription(), message.getId()));
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
        } finally {
            buffer.close();
        }

    }

    @Override
    public ServerSelector getServerSelector() {
        return new ReadPreferenceServerSelector(find.getReadPreference());
    }

    @Override
    public boolean isQuery() {
        return true;
    }
}

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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.session.Session;
import org.mongodb.operation.protocol.MongoGetMoreMessage;
import org.mongodb.operation.protocol.MongoReplyMessage;

import java.nio.ByteBuffer;

public class GetMoreOperation<T> extends Operation {
    private final MongoGetMore getMore;
    private final Decoder<T> resultDecoder;

    public GetMoreOperation(final MongoNamespace namespace, final MongoGetMore getMore, final Decoder<T> resultDecoder,
                            final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
    }


    public QueryResult<T> execute(final Session session) {
        Connection connection = session.getConnection();
        try {
            return execute(connection);
        } finally {
            connection.close();
        }
    }

    public QueryResult<T> executeReceive(final Session session, final long requestId) {
        Connection connection = session.getConnection();
        try {
            return executeReceive(connection, requestId);
        } finally {
            connection.close();
        }
    }

    public void executeDiscard(final Session session) {
        Connection connection = session.getConnection();
        try {
            executeDiscard(connection);
        } finally {
            connection.close();
        }
    }

    public QueryResult<T> execute(final Connection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            final MongoGetMoreMessage message = new MongoGetMoreMessage(getNamespace().getFullName(), getMore);
            message.encode(buffer);
            final ResponseBuffers responseBuffers = connection.sendAndReceiveMessage(buffer);
            try {
                if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                    throw new MongoCursorNotFoundException(new ServerCursor(message.getCursorId(), connection.getServerAddress()));
                }

                if (responseBuffers.getReplyHeader().isQueryFailure()) {
                    final Document errorDocument =
                            new MongoReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId()).getDocuments().get(0);
                    throw new MongoQueryFailureException(connection.getServerAddress(), errorDocument);
                }

                return new QueryResult<T>(new MongoReplyMessage<T>(responseBuffers, resultDecoder, message.getId()),
                        connection.getServerAddress());
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }

    public QueryResult<T> executeReceive(final Connection connection, final long requestId) {
        final ResponseBuffers responseBuffers = connection.receiveMessage();
        try {
            return new QueryResult<T>(new MongoReplyMessage<T>(responseBuffers, resultDecoder, requestId), connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    public void executeDiscard(final Connection connection) {
        long cursorId = getMore.getServerCursor().getId();
        while (cursorId != 0) {
            final ResponseBuffers responseBuffers = connection.receiveMessage();
            try {
               cursorId = responseBuffers.getReplyHeader().getCursorId();
            } finally {
                responseBuffers.close();
            }
        }
    }
}

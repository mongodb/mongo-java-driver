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
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerConnection;
import org.mongodb.operation.protocol.GetMoreMessage;
import org.mongodb.operation.protocol.ReplyMessage;
import org.mongodb.session.Session;

public class GetMoreOperation<T> extends Operation {
    private final GetMore getMore;
    private final Decoder<T> resultDecoder;

    public GetMoreOperation(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                            final BufferProvider bufferProvider) {
        super(namespace, bufferProvider);
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
    }


    public QueryResult<T> execute(final Session session) {
        ServerConnection connection = session.getConnection();
        try {
            return execute(connection);
        } finally {
            connection.close();
        }
    }

    public QueryResult<T> executeReceive(final Session session, final int responseTo) {
        ServerConnection connection = session.getConnection();
        try {
            return executeReceive(connection, responseTo);
        } finally {
            connection.close();
        }
    }

    public void executeDiscard(final Session session, final int responseTo) {
        ServerConnection connection = session.getConnection();
        try {
            executeDiscard(connection, responseTo);
        } finally {
            connection.close();
        }
    }

    public QueryResult<T> execute(final ServerConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
        try {
            final GetMoreMessage message = new GetMoreMessage(getNamespace().getFullName(), getMore,
                    getMessageSettings(connection.getDescription()));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
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
        } finally {
            buffer.close();
        }
    }

    public QueryResult<T> executeReceive(final ServerConnection connection, final int responseTo) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo));
        try {
            return new QueryResult<T>(new ReplyMessage<T>(responseBuffers, resultDecoder, responseTo), connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    public void executeDiscard(final ServerConnection connection, final int responseTo) {
        long cursorId = getMore.getServerCursor().getId();
        int curResponseTo = responseTo;
        while (cursorId != 0) {
            final ResponseBuffers responseBuffers = connection.receiveMessage(
                    getResponseSettings(connection.getDescription(), curResponseTo));
            try {
               cursorId = responseBuffers.getReplyHeader().getCursorId();
               curResponseTo = responseBuffers.getReplyHeader().getRequestId();
            } finally {
                responseBuffers.close();
            }
        }
    }
}

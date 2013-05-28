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

package org.mongodb.operation.async;

import org.mongodb.Decoder;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.protocol.GetMoreMessage;
import org.mongodb.session.AsyncSession;

import java.nio.ByteBuffer;

public class AsyncGetMoreOperation<T> extends AsyncOperation {
    private final MongoGetMore getMore;
    private final Decoder<T> resultDecoder;

    public AsyncGetMoreOperation(final MongoNamespace namespace, final MongoGetMore getMore, final Decoder<T> resultDecoder,
                                 final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
    }

    public MongoFuture<QueryResult<T>> execute(final AsyncSession session) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        session.getConnection().register(new SingleResultCallback<AsyncServerConnection>() {
            @Override
            public void onResult(final AsyncServerConnection connection, final MongoException e) {
                if (e != null) {
                    retVal.init(null, e);
                }
                else {
                    MongoFuture<QueryResult<T>> wrapped = execute(connection);
                    wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
                }
            }
        });

        return retVal;
    }


    public MongoFuture<QueryResult<T>> executeReceive(final AsyncSession session, final int requestId) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        session.getConnection().register(new SingleResultCallback<AsyncServerConnection>() {
            @Override
            public void onResult(final AsyncServerConnection connection, final MongoException e) {
                if (e != null) {
                    retVal.init(null, e);
                }
                else {
                    MongoFuture<QueryResult<T>> wrapped = executeReceive(connection, requestId);
                    wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
                }
            }
        });

        return retVal;

    }

    public MongoFuture<Void> executeDiscard(final AsyncSession session) {
        if (getMore.getServerCursor() == null) {
            return new SingleResultFuture<Void>(null, null);
        }
        else {
            final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

            session.getConnection().register(new SingleResultCallback<AsyncServerConnection>() {
                @Override
                public void onResult(final AsyncServerConnection connection, final MongoException e) {
                    if (e != null) {
                        retVal.init(null, e);
                    }
                    else {
                        MongoFuture<Void> wrapped = executeDiscard(connection);
                        wrapped.register(new ConnectionClosingSingleResultCallback<Void>(connection, retVal));
                    }
                }
            });

            return retVal;
        }
    }


    public MongoFuture<QueryResult<T>> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final GetMoreMessage message = new GetMoreMessage(getNamespace().getFullName(), getMore,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, new GetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, getMore.getServerCursor().getId(), connection,
                message.getId()));

        return retVal;
    }

    public MongoFuture<QueryResult<T>> executeReceive(final AsyncServerConnection connection, final int requestId) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        connection.receiveMessage(new GetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, getMore.getServerCursor().getId(), connection,
                requestId));

        return retVal;
    }

    public MongoFuture<Void> executeDiscard(final AsyncServerConnection connection) {
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

        if (getMore.getServerCursor() == null) {
            retVal.init(null, null);
        }
        else {
            connection.receiveMessage(new DiscardCallback(connection, retVal));
        }

        return retVal;
    }

    private static class DiscardCallback implements SingleResultCallback<ResponseBuffers> {

        private AsyncServerConnection connection;
        private SingleResultFuture<Void> future;

        public DiscardCallback(final AsyncServerConnection connection, final SingleResultFuture<Void> future) {
            this.connection = connection;
            this.future = future;
        }

        @Override
        public void onResult(final ResponseBuffers result, final MongoException e) {
            if (result.getReplyHeader().getCursorId() == 0) {
                future.init(null, null);
            }
            else {
                connection.receiveMessage(new DiscardCallback(connection, future));
            }
        }
    }
}

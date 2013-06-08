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
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.GetMoreMessage;
import org.mongodb.session.AsyncSession;

public class AsyncGetMoreOperation<T> extends AsyncOperation {
    private final GetMore getMore;
    private final Decoder<T> resultDecoder;

    public AsyncGetMoreOperation(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                                 final BufferProvider bufferProvider) {
        super(namespace, bufferProvider);
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


    public MongoFuture<QueryResult<T>> executeReceive(final AsyncSession session, final int responseTo) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        session.getConnection().register(new SingleResultCallback<AsyncServerConnection>() {
            @Override
            public void onResult(final AsyncServerConnection connection, final MongoException e) {
                if (e != null) {
                    retVal.init(null, e);
                }
                else {
                    MongoFuture<QueryResult<T>> wrapped = executeReceive(connection, responseTo);
                    wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
                }
            }
        });

        return retVal;

    }

    public MongoFuture<Void> executeDiscard(final AsyncSession session, final int responseTo) {
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
                        MongoFuture<Void> wrapped = executeDiscard(connection, responseTo);
                        wrapped.register(new ConnectionClosingSingleResultCallback<Void>(connection, retVal));
                    }
                }
            });

            return retVal;
        }
    }


    public MongoFuture<QueryResult<T>> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
        final GetMoreMessage message = new GetMoreMessage(getNamespace().getFullName(), getMore,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, getResponseSettings(connection.getDescription(), message.getId()),
                new GetMoreResultCallback<T>(
                        new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, getMore.getServerCursor().getId(),
                        connection, message.getId()));

        return retVal;
    }

    public MongoFuture<QueryResult<T>> executeReceive(final AsyncServerConnection connection, final int responseTo) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo), new GetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, getMore.getServerCursor().getId(), connection,
                responseTo));

        return retVal;
    }

    public MongoFuture<Void> executeDiscard(final AsyncServerConnection connection, final int responseTo) {
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

        if (getMore.getServerCursor() == null) {
            retVal.init(null, null);
        }
        else {
            connection.receiveMessage(getResponseSettings(connection.getDescription(), responseTo),
                    new DiscardCallback(connection, retVal, responseTo));
        }

        return retVal;
    }

    private static class DiscardCallback implements SingleResultCallback<ResponseBuffers> {

        private AsyncServerConnection connection;
        private SingleResultFuture<Void> future;
        private int responseTo;

        public DiscardCallback(final AsyncServerConnection connection, final SingleResultFuture<Void> future,
                               final int responseTo) {
            this.connection = connection;
            this.future = future;
            this.responseTo = responseTo;
        }

        @Override
        public void onResult(final ResponseBuffers result, final MongoException e) {
            if (result.getReplyHeader().getCursorId() == 0) {
                future.init(null, null);
            }
            else {
                connection.receiveMessage(AsyncGetMoreOperation.getResponseSettings(connection.getDescription(), responseTo),
                        new DiscardCallback(connection, future, result.getReplyHeader().getRequestId()));
            }
        }
    }
}

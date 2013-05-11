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

package org.mongodb.async;

import org.mongodb.Decoder;
import org.mongodb.MongoConnectionManager;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoServerBinding;
import org.mongodb.impl.MongoAsyncConnection;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.result.QueryResult;

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

    public MongoFuture<QueryResult<T>> execute(final MongoServerBinding binding) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(getMore.getServerCursor().getAddress());
        MongoAsyncConnection connection = connectionManager.getAsyncConnection();

        MongoFuture<QueryResult<T>> wrapped = execute(connection);
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
        return retVal;
    }


    public MongoFuture<QueryResult<T>> executeReceive(final MongoServerBinding binding) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(getMore.getServerCursor().getAddress());
        MongoAsyncConnection connection = connectionManager.getAsyncConnection();

        MongoFuture<QueryResult<T>> wrapped = executeReceive(connection);
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
        return retVal;
    }

    public MongoFuture<Void> executeDiscard(final MongoServerBinding binding) {
        if (getMore.getServerCursor() == null) {
            return new SingleResultFuture<Void>(null, null);
        }
        else {
            MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(getMore.getServerCursor().getAddress());
            MongoAsyncConnection connection = connectionManager.getAsyncConnection();

            MongoFuture<Void> wrapped = executeDiscard(connection);
            SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
            wrapped.register(new ConnectionClosingSingleResultCallback<Void>(connection, retVal));
            return retVal;
        }
    }


    public MongoFuture<QueryResult<T>> execute(final MongoAsyncConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final MongoGetMoreMessage message = new MongoGetMoreMessage(getNamespace().getFullName(), getMore);
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, new MongoGetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, getMore.getServerCursor().getId(), connection));

        return retVal;
    }

    public MongoFuture<QueryResult<T>> executeReceive(final MongoAsyncConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        connection.receiveMessage(new MongoGetMoreResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, getMore.getServerCursor().getId(), connection));

        return retVal;
    }

    public MongoFuture<Void> executeDiscard(final MongoAsyncConnection connection) {
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

        private MongoAsyncConnection connection;
        private SingleResultFuture<Void> future;

        public DiscardCallback(final MongoAsyncConnection connection, final SingleResultFuture<Void> future) {
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

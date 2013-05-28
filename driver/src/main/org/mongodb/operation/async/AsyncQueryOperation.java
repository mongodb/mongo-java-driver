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
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.Find;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.protocol.QueryMessage;
import org.mongodb.session.AsyncSession;

import java.nio.ByteBuffer;

public class AsyncQueryOperation<T> extends AsyncOperation {
    private final Find find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;

    public AsyncQueryOperation(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                               final Decoder<T> resultDecoder, final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
        this.find = find;
        this.queryEncoder = queryEncoder;
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

    public MongoFuture<QueryResult<T>> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final QueryMessage message = new QueryMessage(getNamespace().getFullName(), find, queryEncoder,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, getResponseSettings(connection.getDescription(), message.getId()),
                new QueryResultCallback<T>(
                        new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, connection, message.getId()));

        return retVal;
    }
}
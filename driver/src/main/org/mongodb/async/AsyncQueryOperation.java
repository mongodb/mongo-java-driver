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

import org.mongodb.Cluster;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.Server;
import org.mongodb.impl.MongoAsyncConnection;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.operation.MongoFind;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.result.QueryResult;

import java.nio.ByteBuffer;

public class AsyncQueryOperation<T> extends AsyncOperation {
    private final MongoFind find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;

    public AsyncQueryOperation(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                               final Decoder<T> resultDecoder, final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.resultDecoder = resultDecoder;
    }

    public MongoFuture<QueryResult<T>> execute(final Cluster cluster) {
        Server connectionManager = cluster.getConnectionManagerForRead(find.getReadPreference());
        MongoAsyncConnection connection = connectionManager.getAsyncConnection();

        MongoFuture<QueryResult<T>> wrapped = execute(connection);
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
        return retVal;
    }

    public MongoFuture<QueryResult<T>> execute(final MongoAsyncConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final MongoQueryMessage message = new MongoQueryMessage(getNamespace().getFullName(), find, queryEncoder);
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, new MongoQueryResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, connection));

        return retVal;
    }
}
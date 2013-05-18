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
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.AsyncSession;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.protocol.MongoQueryMessage;

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

    public MongoFuture<QueryResult<T>> execute(final AsyncSession session) {
        AsyncConnection connection = session.getConnection(new ReadPreferenceServerSelector(find.getReadPreference()));

        MongoFuture<QueryResult<T>> wrapped = execute(connection);
        SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();
        wrapped.register(new ConnectionClosingSingleResultCallback<QueryResult<T>>(connection, retVal));
        return retVal;
    }

    public MongoFuture<QueryResult<T>> execute(final AsyncConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final MongoQueryMessage message = new MongoQueryMessage(getNamespace().getFullName(), find, queryEncoder);
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, new MongoQueryResultCallback<T>(
                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, connection, message.getId()));

        return retVal;
    }
}
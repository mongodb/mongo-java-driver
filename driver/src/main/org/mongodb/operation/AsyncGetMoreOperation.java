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

import org.mongodb.AsyncOperation;
import org.mongodb.Decoder;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.operation.protocol.GetMoreMessage;
import org.mongodb.operation.protocol.QueryResult;

import static org.mongodb.operation.OperationHelpers.encodeMessageToBuffer;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;

public class AsyncGetMoreOperation<T> implements AsyncOperation<QueryResult<T>> {
    private final GetMore getMore;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public AsyncGetMoreOperation(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                                 final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.getMore = getMore;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public MongoFuture<QueryResult<T>> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        final GetMoreMessage message = new GetMoreMessage(namespace.getFullName(), getMore,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendMessage(buffer.getByteBuffers(),
                new SendMessageCallback<QueryResult<T>>(connection, buffer, message.getId(), retVal,
                        new GetMoreResultCallback<T>(
                                new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder,
                                getMore.getServerCursor().getId(), connection, message.getId())));
        return retVal;
    }
}

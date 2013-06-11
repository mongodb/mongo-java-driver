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

import org.mongodb.AsyncServerSelectingOperation;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerSelector;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.QueryMessage;

import static org.mongodb.operation.OperationHelpers.encodeMessageToBuffer;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class AsyncQueryOperation<T> implements AsyncServerSelectingOperation<QueryResult<T>> {
    private final Find find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public AsyncQueryOperation(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                               final Decoder<T> resultDecoder, final BufferProvider bufferProvider) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.find = find;
        this.queryEncoder = queryEncoder;
        this.resultDecoder = resultDecoder;
    }

    @Override
    public MongoFuture<QueryResult<T>> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        final QueryMessage message = new QueryMessage(namespace.getFullName(), find, queryEncoder,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendMessage(buffer.getByteBuffers(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                buffer.close();
                if (e != null) {
                    retVal.init(null, e);
                }
                else {
                    connection.receiveMessage(getResponseSettings(connection.getDescription(), message.getId()),
                            new QueryResultCallback<T>(
                                    new SingleResultFutureCallback<QueryResult<T>>(retVal), resultDecoder, connection,
                                    message.getId()));
                }
            }
        });
        return retVal;
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
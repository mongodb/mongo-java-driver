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

package org.mongodb;

import org.mongodb.impl.MongoSyncConnection;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;

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


    public QueryResult<T> execute(final MongoServerBinding binding) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(getMore.getServerCursor().getAddress());
        MongoSyncConnection connection = connectionManager.getConnection();
        try {
            return execute(connection);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    public QueryResult<T> execute(final MongoSyncConnection connection) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        try {
            final MongoGetMoreMessage message = new MongoGetMoreMessage(getNamespace().getFullName(), getMore);
            message.encode(buffer);
            final ResponseBuffers responseBuffers = connection.sendAndReceiveMessage(buffer);
            try {
                if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                    throw new MongoCursorNotFoundException(new ServerCursor(message.getCursorId(), connection.getServerAddress()));
                }

                final MongoReplyMessage<T> replyMessage = new MongoReplyMessage<T>(responseBuffers, resultDecoder);
                return new QueryResult<T>(replyMessage, connection.getServerAddress());
            } finally {
                responseBuffers.close();
            }
        } finally {
            buffer.close();
        }
    }
}

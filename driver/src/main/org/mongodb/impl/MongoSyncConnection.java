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

package org.mongodb.impl;

import org.mongodb.Codec;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerAddress;
import org.mongodb.command.MongoCommand;
import org.mongodb.io.BufferPool;
import org.mongodb.io.CachingAuthenticator;
import org.mongodb.io.MongoGateway;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.pool.SimplePool;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;

import java.nio.ByteBuffer;
import java.util.List;

final class MongoSyncConnection implements MongoConnection {
    private final BufferPool<ByteBuffer> bufferPool;
    private final MongoClientOptions options;
    private final SimplePool<MongoConnection> channelPool;
    private MongoGateway channel;

    MongoSyncConnection(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                        final SimplePool<MongoConnection> channelPool, final BufferPool<ByteBuffer> bufferPool,
                        final MongoClientOptions options) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.options = options;
        this.channel = MongoGateway.create(serverAddress, bufferPool, options,
                new CachingAuthenticator(new MongoCredentialsStore(credentialList), this));
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public ServerAddress getServerAddress() {
        return channel.getAddress();
    }

    @Override
    public MongoGateway getGateway() {
        return channel;
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    @Override
    public void release() {
        if (channel == null) {
            throw new IllegalStateException("Can not release a channel that's already closed");
        }
        if (channelPool == null) {
            throw new IllegalStateException("Can not release a channel not associated with a pool");
        }

        channelPool.done(this);
    }

    @Override
    public MongoFuture<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                                   final Codec<Document> codec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                      final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final MongoGetMore getMore,
                                                        final Decoder<T> resultDecoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> MongoFuture<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                                    final Encoder<T> encoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoFuture<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                                final Encoder<Document> queryEncoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> MongoFuture<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                     final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoFuture<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                                final Encoder<Document> queryEncoder) {
        throw new UnsupportedOperationException();
    }

}

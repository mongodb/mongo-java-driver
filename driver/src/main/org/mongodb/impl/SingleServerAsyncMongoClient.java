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
 *
 */

package org.mongodb.impl;

import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerAddress;
import org.mongodb.async.MongoAsyncOperations;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.io.async.MongoAsynchronousChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.pool.SimplePool;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class SingleServerAsyncMongoClient extends SingleServerMongoClient {
    private final ServerAddress serverAddress;
    private final SimplePool<MongoAsynchronousChannel> channelPool;
    private final SingleServerAsyncMongoClient.SingleServerAsyncMongoOperations operations;

    public SingleServerAsyncMongoClient(final ServerAddress serverAddress, final MongoClientOptions options) {
        this(serverAddress, options, new PowerOfTwoByteBufferPool());
    }

    public SingleServerAsyncMongoClient(final ServerAddress serverAddress, final MongoClientOptions options,
                                        final BufferPool<ByteBuffer> bufferPool) {
        super(options, bufferPool, serverAddress);
        this.serverAddress = serverAddress;
        channelPool = new SimplePool<MongoAsynchronousChannel>(serverAddress.toString(),
                                                              getOptions().getConnectionsPerHost()) {
            @Override
            protected MongoAsynchronousChannel createNew() {
                return new MongoAsynchronousChannel(SingleServerAsyncMongoClient.this.serverAddress, getBufferPool(),
                                                   new DocumentSerializer(getOptions().getPrimitiveSerializers()));
            }
        };
        operations = new SingleServerAsyncMongoOperations();
    }

    @Override
    public MongoAsyncOperations getAsyncOperations() {
        return operations;
    }

    @Override
    public void close() {
        channelPool.close();
    }

    protected SingleChannelMongoClient createSingleChannelMongoClient() {
        return new SingleChannelAsyncMongoClient(serverAddress, getChannelPool(), getBufferPool(), getOptions());
    }

    SimplePool<MongoAsynchronousChannel> getChannelPool() {
        return channelPool;
    }

    private class SingleServerAsyncMongoOperations implements MongoAsyncOperations {
        @Override
        public Future<CommandResult> asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                                         final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncExecuteCommand(database, commandOperation, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public void asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                        final Serializer<Document> serializer,
                                        final SingleResultCallback<CommandResult> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncExecuteCommand(database, commandOperation, serializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                     final Serializer<Document> baseSerializer,
                                                     final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncQuery(namespace, find, baseSerializer, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                   final Serializer<Document> baseSerializer,
                                   final Serializer<T> serializer,
                                   final SingleResultCallback<QueryResult<T>> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncQuery(namespace, find, baseSerializer, serializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                       final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncGetMore(namespace, getMore, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                     final Serializer<T> serializer,
                                     final SingleResultCallback<QueryResult<T>> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncGetMore(namespace, getMore, serializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                                   final Serializer<T> serializer,
                                                   final Serializer<Document> baseSerializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncInsert(namespace, insert, serializer, baseSerializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                    final Serializer<T> serializer,
                                    final Serializer<Document> baseSerializer,
                                    final SingleResultCallback<WriteResult> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncInsert(namespace, insert, serializer, baseSerializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                               final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncUpdate(namespace, update, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                final Serializer<Document> serializer,
                                final SingleResultCallback<WriteResult> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncUpdate(namespace, update, serializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                    final Serializer<Document> baseSerializer,
                                                    final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncReplace(namespace, replace, baseSerializer, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                     final Serializer<Document> baseSerializer, final Serializer<T> serializer,
                                     final SingleResultCallback<WriteResult> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncReplace(namespace, replace, baseSerializer, serializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                               final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getAsyncOperations().asyncRemove(namespace, remove, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                final Serializer<Document> serializer,
                                final SingleResultCallback<WriteResult> callback) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getAsyncOperations().asyncRemove(namespace, remove, serializer, callback);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }
    }
}

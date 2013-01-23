/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.ClientAdmin;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.ServerAddress;
import org.mongodb.io.MongoChannel;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.pool.SimplePool;
import org.mongodb.result.CommandResult;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * An implementation of {@code MongoClient} that represents a connection to a single server.
 */
class SingleServerMongoClient implements MongoClient {

    private final ServerAddress serverAddress;
    private final MongoClientOptions options;
    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool(24);
    private final ThreadLocal<SingleChannelMongoClient> boundClient = new ThreadLocal<SingleChannelMongoClient>();
    private final ClientAdmin admin;

    public SingleServerMongoClient(final ServerAddress serverAddress) {
        this(serverAddress, MongoClientOptions.builder().build());
    }

    public SingleServerMongoClient(final ServerAddress serverAddress, final MongoClientOptions options) {
        this.serverAddress = serverAddress;
        this.options = options;
        channelPool = new SimplePool<MongoChannel>(serverAddress.toString(), options.getConnectionsPerHost()) {
            @Override
            protected MongoChannel createNew() {
                return new MongoChannel(SingleServerMongoClient.this.serverAddress, bufferPool,
                                        new DocumentSerializer(options.getPrimitiveSerializers()));
            }
        };
        admin = new ClientAdminImpl(getOperations(), options.getPrimitiveSerializers());
    }

    @Override
    public MongoDatabaseImpl getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabaseImpl getDatabase(final String databaseName, final MongoDatabaseOptions optionsForOperation) {
        return new MongoDatabaseImpl(databaseName, this, optionsForOperation.withDefaults(this.getOptions()));
    }

    @Override
    public MongoOperations getOperations() {
        //TODO: we're not caching this?
        return new SingleServerMongoOperations();
    }


    @Override
    public void withConnection(final Runnable runnable) {
        boundClient.set(getChannelClient());
        try {
            runnable.run();
        } finally {
            releaseChannelClient(boundClient.get());
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        boundClient.set(getChannelClient());
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            releaseChannelClient(boundClient.get());
        }
    }

    /**
     * Bind to a connection, but not lexically scoped.  This is not part of the public MongoClient API, as this is only
     * offered so that com.mongodb.DB#requestStart can be implemented.
     */
    public void bindToConnection() {
        boundClient.set(getChannelClient());

    }

    /**
     * Unbind from a connection, but not lexically scoped.  This is not part of the public MongoClient API, as this is
     * only offered so that com.mongodb.DB#requestDone can be implemented.
     */
    public void unbindFromConnection() {
        releaseChannelClient(boundClient.get());
    }

    @Override
    public void close() {
        channelPool.close();
    }

    @Override
    public MongoClientOptions getOptions() {
        return options;
    }

    @Override
    public ClientAdmin admin() {
        return admin;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    SimplePool<MongoChannel> getChannelPool() {
        return channelPool;
    }

    private SingleChannelMongoClient getChannelClient() {
        if (boundClient.get() != null) {
            return boundClient.get();
        }
        return new SingleChannelMongoClient(getChannelPool(), getBufferPool(), options);
    }

    private void releaseChannelClient(final SingleChannelMongoClient mongoClient) {
        if (boundClient.get() != null) {
            if (boundClient.get() != mongoClient) {
                throw new IllegalArgumentException("Can't unbind from a different client than you are bound to");
            }
            boundClient.remove();
        }
        else {
            mongoClient.close();
        }
    }


    private class SingleServerMongoOperations implements MongoOperations {
        @Override
        public CommandResult executeCommand(final String database, final MongoCommand commandOperation,
                                            final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().executeCommand(database, commandOperation, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().query(namespace, find, baseSerializer, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                            final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().getMore(namespace, getMore, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                       final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().insert(namespace, insert, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, final MongoUpdate update,
                                   final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().update(namespace, update, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> UpdateResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().replace(namespace, replace, baseSerializer, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public RemoveResult remove(final MongoNamespace namespace, final MongoRemove remove,
                                   final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().remove(namespace, remove, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public void killCursors(final MongoKillCursor killCursor) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getOperations().killCursors(killCursor);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }
    }
}

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
import org.mongodb.MongoOperations;
import org.mongodb.ServerAddress;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public abstract class SingleServerMongoClient extends AbstractMongoClient {
    private final ServerAddress serverAddress;
    private final ThreadLocal<SingleChannelMongoClient> boundClient = new ThreadLocal<SingleChannelMongoClient>();

    SingleServerMongoClient(final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool, final ServerAddress serverAddress) {
        super(options, bufferPool);
        this.serverAddress = serverAddress;
    }

    @Override
    List<ServerAddress> getServerAddressList() {
        return Arrays.asList(serverAddress);
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    protected ThreadLocal<SingleChannelMongoClient> getBoundClient() {
        return boundClient;
    }

    protected abstract SingleChannelMongoClient createSingleChannelMongoClient();

    @Override
    public void withConnection(final Runnable runnable) {
        getBoundClient().set(getChannelClient());
        try {
            runnable.run();
        } finally {
            releaseChannelClient(getBoundClient().get());
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        getBoundClient().set(getChannelClient());
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            releaseChannelClient(getBoundClient().get());
        }
    }

    /**
     * Bind to a connection, but not lexically scoped.  This is not part of the public MongoClient API, as this is only
     * offered so that com.mongodb.DB#requestStart can be implemented.
     */
    public void bindToConnection() {
        getBoundClient().set(getChannelClient());
    }

    /**
     * Unbind from a connection, but not lexically scoped.  This is not part of the public MongoClient API, as this is
     * only offered so that com.mongodb.DB#requestDone can be implemented.
     */
    public void unbindFromConnection() {
        releaseChannelClient(getBoundClient().get());
    }

    @Override
    public MongoOperations getOperations() {
        return new SingleServerMongoOperations();
    }

    protected SingleChannelMongoClient getChannelClient() {
        if (getBoundClient().get() != null) {
            return getBoundClient().get();
        }
        return createSingleChannelMongoClient();
    }

    protected void releaseChannelClient(final SingleChannelMongoClient mongoClient) {
        if (getBoundClient().get() != null) {
            if (getBoundClient().get() != mongoClient) {
                throw new IllegalArgumentException("Can't unbind from a different client than you are bound to");
            }
            getBoundClient().remove();
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
        public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                      final Serializer<T> serializer, final Serializer<Document> baseSerializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().insert(namespace, insert, serializer, baseSerializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public WriteResult update(final MongoNamespace namespace, final MongoUpdate update,
                                  final Serializer<Document> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().update(namespace, update, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                       final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            final SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().replace(namespace, replace, baseSerializer, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
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

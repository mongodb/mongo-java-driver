/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.impl;

import org.bson.util.BufferPool;
import org.bson.util.PowerOfTwoByteBufferPool;
import org.mongodb.MongoClient;
import org.mongodb.MongoDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.util.pool.SimplePool;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

// TODO: should this be a public class?
public class SingleServerMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool(24);
    private final ServerAddress serverAddress;
    private final PrimitiveSerializers primitiveSerializers;
    private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    private ReadPreference readPreference = ReadPreference.primary();
    private final ThreadLocal<SingleChannelMongoClient> boundClient = new ThreadLocal<SingleChannelMongoClient>();

    public SingleServerMongoClient(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        channelPool = new SimplePool<MongoChannel>(serverAddress.toString(), 100) {
            @Override
            protected MongoChannel createNew() {
                return new MongoChannel(SingleServerMongoClient.this.serverAddress, bufferPool);
            }
        };
        primitiveSerializers = PrimitiveSerializers.createDefault();
    }

    @Override
    public MongoDatabaseImpl getDatabase(final String name) {
        return new MongoDatabaseImpl(name, this);
    }

    @Override
    public MongoOperations getOperations() {
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
     * Unbind from a connection, but not lexically scoped.  This is not part of the public MongoClient API, as this is only
     * offered so that com.mongodb.DB#requestDone can be implemented.
     */
    public void unbindFromConnection() {
        releaseChannelClient(boundClient.get());
    }

    @Override
    public void close() {
        // TODO: close pool, release buffers
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
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
        return new SingleChannelMongoClient(getChannelPool(), getBufferPool(), primitiveSerializers, writeConcern, readPreference);
    }

    private void releaseChannelClient(SingleChannelMongoClient mongoClient) {
        if (boundClient.get() != null) {
            if (boundClient.get() != mongoClient) {
                throw new IllegalArgumentException("Can't unbind from a different client than you are bound to");
            }
            boundClient.remove();
        } else {
            mongoClient.close();
        }
    }


    private class SingleServerMongoOperations implements MongoOperations {
        @Override
        public MongoDocument executeCommand(final String database, final MongoCommandOperation commandOperation,
                                            Serializer<MongoDocument> serializer) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().executeCommand(database, commandOperation, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                        Serializer<MongoDocument> baseSerializer,
                                        Serializer<T> serializer) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().query(namespace, find, baseSerializer, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, GetMore getMore, Serializer<T> serializer) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().getMore(namespace, getMore, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                       Serializer<T> serializer) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().insert(namespace, insert, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, MongoUpdate update, Serializer<MongoDocument> serializer) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().update(namespace, update, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public RemoveResult remove(final MongoNamespace namespace, final MongoRemove remove,
                                   Serializer<MongoDocument> serializer) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                return mongoClient.getOperations().remove(namespace, remove, serializer);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }

        @Override
        public void killCursors(MongoKillCursor killCursor) {
            SingleChannelMongoClient mongoClient = getChannelClient();
            try {
                mongoClient.getOperations().killCursors(killCursor);
            } finally {
                releaseChannelClient(mongoClient);
            }
        }
    }
}

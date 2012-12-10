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

import org.bson.BsonType;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.util.BufferPool;
import org.bson.util.PowerOfTwoByteBufferPool;
import org.mongodb.MongoClient;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.MongoDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.BinarySerializer;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;
import org.mongodb.serialization.serializers.BooleanSerializer;
import org.mongodb.serialization.serializers.DateSerializer;
import org.mongodb.serialization.serializers.DoubleSerializer;
import org.mongodb.serialization.serializers.IntegerSerializer;
import org.mongodb.serialization.serializers.LongSerializer;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;
import org.mongodb.serialization.serializers.ObjectIdSerializer;
import org.mongodb.serialization.serializers.StringSerializer;
import org.mongodb.util.pool.SimplePool;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

// TODO: should this be a public class?
public class SingleServerMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool(24);
    private final ServerAddress serverAddress;
    private final Serializer serializer;
    private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    private ReadPreference readPreference = ReadPreference.primary();
    private final ThreadLocal<MongoClient> boundClient = new ThreadLocal<MongoClient>();

    public SingleServerMongoClient(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        channelPool = new SimplePool<MongoChannel>(serverAddress.toString(), 100) {
            @Override
            protected MongoChannel createNew() {
                return new MongoChannel(SingleServerMongoClient.this.serverAddress, bufferPool);
            }
        };
        serializer = createDefaultSerializer();
    }

    // TODO: find a better home for this.
    Serializer createDefaultSerializer() {
        Serializers serializers = new Serializers();
        serializers.register(MongoDocument.class, BsonType.DOCUMENT, new MongoDocumentSerializer(serializers));
        serializers.register(ObjectId.class, BsonType.OBJECT_ID, new ObjectIdSerializer());
        serializers.register(Integer.class, BsonType.INT32, new IntegerSerializer());
        serializers.register(Long.class, BsonType.INT64, new LongSerializer());
        serializers.register(String.class, BsonType.STRING, new StringSerializer());
        serializers.register(Double.class, BsonType.DOUBLE, new DoubleSerializer());
        serializers.register(Binary.class, BsonType.BINARY, new BinarySerializer());
        serializers.register(Date.class, BsonType.DATE_TIME, new DateSerializer());
        serializers.register(Boolean.class, BsonType.BOOLEAN, new BooleanSerializer());
        return serializers;
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
        boundClient.set(new SingleChannelMongoClient(getChannelPool(), getBufferPool(), serializer, writeConcern, readPreference));
        try {
            runnable.run();
        } finally {
            boundClient.remove();
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        boundClient.set(new SingleChannelMongoClient(getChannelPool(), getBufferPool(), serializer, writeConcern, readPreference));
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            boundClient.remove();
        }
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

    BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    SimplePool<MongoChannel> getChannelPool() {
        return channelPool;
    }

    private MongoClient bind() {
        if (boundClient.get() != null) {
            return boundClient.get();
        }
        return new SingleChannelMongoClient(getChannelPool(), getBufferPool(), serializer, writeConcern, readPreference);
    }

    private void unbind(MongoClient mongoClient) {
        if (boundClient.get() != null) {
           if (boundClient.get() != mongoClient) {
               throw new IllegalArgumentException("Can't unbind from a different client than you are bound to");
           }
        }
        else {
            mongoClient.close();
        }
    }


    private class SingleServerMongoOperations implements MongoOperations {
        @Override
        public CommandResult executeCommand(final String database, final MongoCommandOperation commandOperation) {
            MongoClient mongoClient = bind();
            try {
                return mongoClient.getOperations().executeCommand(database, commandOperation);
            } finally {
                unbind(mongoClient);
            }
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find, final Class<T> clazz) {
            MongoClient mongoClient = bind();
            try {
                return mongoClient.getOperations().query(namespace, find, clazz);
            } finally {
                unbind(mongoClient);
            }
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, GetMore getMore, final Class<T> clazz) {
            MongoClient mongoClient = bind();
            try {
                return mongoClient.getOperations().getMore(namespace, getMore, clazz);
            } finally {
                unbind(mongoClient);
            }
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, Class<T> clazz) {
            MongoClient mongoClient = bind();
            try {
                return mongoClient.getOperations().insert(namespace, insert, clazz);
            } finally {
                unbind(mongoClient);
            }
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, MongoUpdate update) {
            MongoClient mongoClient = bind();
            try {
                return mongoClient.getOperations().update(namespace, update);
            } finally {
                unbind(mongoClient);
            }
        }

        @Override
        public RemoveResult delete(final MongoNamespace namespace, final MongoRemove remove) {
            MongoClient mongoClient = bind();
            try {
                return mongoClient.getOperations().delete(namespace, remove);
            } finally {
                unbind(mongoClient);
            }
        }

        @Override
        public void killCursors(MongoKillCursor killCursor) {
            MongoClient mongoClient = bind();
            try {
                mongoClient.getOperations().killCursors(killCursor);
            } finally {
                unbind(mongoClient);
            }
        }
    }
}

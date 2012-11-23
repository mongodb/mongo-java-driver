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
import org.mongodb.CommandResult;
import org.mongodb.InsertResult;
import org.mongodb.MongoChannel;
import org.mongodb.MongoClient;
import org.mongodb.MongoDocument;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.mongodb.serialization.BinarySerializer;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;
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

class SingleServerMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool(24);
    private final ServerAddress serverAddress;
    private final Serializer serializer;

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
        return serializers;
    }

    @Override
    public MongoDatabaseImpl getDatabase(final String name) {
        return new MongoDatabaseImpl(name, this);
    }

    @Override
    public CommandResult executeCommand(final String database, final MongoDocument command) {
        MongoClient mongoClient = new SingleChannelMongoClient(getChannelPool(), getBufferPool(), serializer);
        try {
            return mongoClient.executeCommand(database, command);
        } finally {
            mongoClient.close();
        }
    }

    @Override
    public <T> InsertResult insert(final String namespace, final T doc, final WriteConcern writeConcern) {
        MongoClient mongoClient = new SingleChannelMongoClient(getChannelPool(), getBufferPool(), serializer);
        try {
            return mongoClient.insert(namespace, doc, writeConcern);
        } finally {
            mongoClient.close();
        }
    }

    @Override
    public void close() {
        // TODO: close pool, release buffers
    }

    BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    SimplePool<MongoChannel> getChannelPool() {
        return channelPool;
    }
}

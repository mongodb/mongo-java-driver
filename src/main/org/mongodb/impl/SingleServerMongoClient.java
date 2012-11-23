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
import org.bson.io.PooledByteBufferOutput;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.util.BufferPool;
import org.bson.util.PowerOfTwoByteBufferPool;
import org.mongodb.CommandResult;
import org.mongodb.InsertResult;
import org.mongodb.MongoChannel;
import org.mongodb.MongoClient;
import org.mongodb.MongoDocument;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyMessage;
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

import java.io.IOException;
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
        try {
            MongoChannel channel = channelPool.get();
            try {
                MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd", 0, 0, -1,
                        command, null, ReadPreference.primary(), new PooledByteBufferOutput(bufferPool), serializer);
                channel.sendMessage(message);

                MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializer, MongoDocument.class);

                return new CommandResult(replyMessage.getDocuments().get(0));
            } catch (IOException e) {
                throw new MongoException("", e);
            } finally {
                channelPool.done(channel);
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(e);
        }
    }

    @Override
    public <T> InsertResult insert(final String namespace, final T doc, final WriteConcern writeConcern, final Serializer serializer) {
        MongoInsertMessage insertMessage = new MongoInsertMessage(namespace, writeConcern,
                new PooledByteBufferOutput(getBufferPool()));
        insertMessage.addDocument(doc.getClass(), doc, serializer);

        MongoChannel mongoChannel = null;
        try {
            mongoChannel = getChannelPool().get();
            mongoChannel.sendMessage(insertMessage);
            return null;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(e);
        } catch (IOException e) {
            throw new MongoException("insert", e);
        } finally {
            if (mongoChannel != null) {
                getChannelPool().done(mongoChannel);
            }
        }

    }

    BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    SimplePool<MongoChannel> getChannelPool() {
        return channelPool;
    }
}

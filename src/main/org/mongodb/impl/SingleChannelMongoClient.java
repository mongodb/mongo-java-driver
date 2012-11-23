/**
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
 *
 */

package org.mongodb.impl;

import org.bson.io.PooledByteBufferOutput;
import org.bson.util.BufferPool;
import org.mongodb.CommandResult;
import org.mongodb.InsertResult;
import org.mongodb.MongoChannel;
import org.mongodb.MongoClient;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDocument;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.serialization.Serializer;
import org.mongodb.util.pool.SimplePool;

import java.io.IOException;
import java.nio.ByteBuffer;

class SingleChannelMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool;
    private final Serializer serializer;
    private MongoChannel channel;

    SingleChannelMongoClient(final SimplePool<MongoChannel> channelPool, final BufferPool<ByteBuffer> bufferPool,
                             final Serializer serializer) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.serializer = serializer;
        try {
            this.channel = channelPool.get();
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(e);
        }
    }


    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(name, this);
    }

    @Override
    public CommandResult executeCommand(final String database, final MongoDocument command) {
        try {
            MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd", 0, 0, -1,
                    command, null, ReadPreference.primary(), new PooledByteBufferOutput(bufferPool), serializer);
            channel.sendMessage(message);

            MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializer, MongoDocument.class);

            return new CommandResult(replyMessage.getDocuments().get(0));
        } catch (IOException e) {
            throw new MongoException("", e);
        }
    }

    @Override
    public <T> InsertResult insert(final String namespace, final T doc, final WriteConcern writeConcern) {
        MongoInsertMessage insertMessage = new MongoInsertMessage(namespace, writeConcern,
                new PooledByteBufferOutput(getBufferPool()));
        insertMessage.addDocument(doc.getClass(), doc, serializer);

        try {
            channel.sendMessage(insertMessage);
            return null;
        } catch (IOException e) {
            throw new MongoException("insert", e);
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            channelPool.done(channel);
            channel = null;
        }
    }

    private BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }
}

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
import org.mongodb.DeleteResult;
import org.mongodb.GetMoreResult;
import org.mongodb.InsertResult;
import org.mongodb.MongoChannel;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollectionName;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDocument;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoOperations;
import org.mongodb.QueryResult;
import org.mongodb.ReadPreference;
import org.mongodb.UpdateResult;
import org.mongodb.WriteConcern;
import org.mongodb.protocol.MongoDeleteMessage;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoKillCursorsMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.protocol.MongoUpdateMessage;
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
    public MongoOperations getOperations() {
        return new SingleChannelMongoOperations();
    }


    @Override
    public MongoClient bindToChannel() {
        return this;
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

    private <T> MongoReplyMessage<MongoDocument> sendWriteMessage(final MongoCollectionName namespace,
                                                                  final MongoRequestMessage writeMessage,
                                                                  final WriteConcern writeConcern) {
        try {
            channel.sendMessage(writeMessage);
            if (writeConcern.callGetLastError()) {
                MongoQueryMessage getLastErrorMessage = new MongoQueryMessage(namespace.getFullName(),
                        0, 0, -1, writeConcern.getCommand(), null, null, new PooledByteBufferOutput(getBufferPool()),
                        serializer);
                channel.sendMessage(getLastErrorMessage);
                MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializer, MongoDocument.class);
                return replyMessage;
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new MongoException("insert", e);
        }
    }

    private class SingleChannelMongoOperations implements MongoOperations {
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
        public <T> InsertResult insert(final MongoCollectionName namespace, final T doc, final WriteConcern writeConcern) {
            MongoInsertMessage insertMessage = new MongoInsertMessage(namespace.getFullName(), writeConcern,
                    new PooledByteBufferOutput(getBufferPool()));
            insertMessage.addDocument(doc.getClass(), doc, serializer);

            return new InsertResult(sendWriteMessage(namespace, insertMessage, writeConcern));
        }

        @Override
        public <T> QueryResult<T> query(final MongoCollectionName namespace, final MongoDocument query, Class<T> clazz) {
            try {
                MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), 0, 0, 0,
                        query, null, ReadPreference.primary(), new PooledByteBufferOutput(bufferPool), serializer);
                channel.sendMessage(message);

                MongoReplyMessage<T> replyMessage = channel.receiveMessage(serializer, clazz);

                return new QueryResult<T>(replyMessage);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoCollectionName namespace, long cursorId, Class<T> clazz) {
            try {
                MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), cursorId, 0,
                        new PooledByteBufferOutput(bufferPool));
                channel.sendMessage(message);

                MongoReplyMessage<T> replyMessage = channel.receiveMessage(serializer, clazz);

                return new GetMoreResult<T>(replyMessage);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public UpdateResult update(final MongoCollectionName namespace, MongoDocument query,
                                   MongoDocument updateOperations, WriteConcern writeConcern) {
            MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), false, false, query,
                    updateOperations, new PooledByteBufferOutput(bufferPool), serializer);
            return new UpdateResult(sendWriteMessage(namespace, message, writeConcern));
        }

        @Override
        public DeleteResult delete(final MongoCollectionName namespace, MongoDocument query,
                                   WriteConcern writeConcern) {
            MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), query,
                    new PooledByteBufferOutput(bufferPool), serializer);
            return new DeleteResult(sendWriteMessage(namespace, message, writeConcern));
        }

        @Override
        public void killCursors(final long cursorId, final long... cursorIds) {
            try {
                MongoKillCursorsMessage message = new MongoKillCursorsMessage(new PooledByteBufferOutput(bufferPool),
                        cursorId, cursorIds);
                channel.sendMessage(message);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }
    }
}

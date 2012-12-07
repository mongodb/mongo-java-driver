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
import org.mongodb.*;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoDelete;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoQuery;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.operation.MongoWrite;
import org.mongodb.protocol.MongoDeleteMessage;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoKillCursorsMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.protocol.MongoUpdateMessage;
import org.mongodb.result.CommandResult;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.util.pool.SimplePool;

import java.io.IOException;
import java.nio.ByteBuffer;

class SingleChannelMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool;
    private final Serializer serializer;
    private final WriteConcern writeConcern;
    private MongoChannel channel;

    SingleChannelMongoClient(final SimplePool<MongoChannel> channelPool, final BufferPool<ByteBuffer> bufferPool,
                             final Serializer serializer, final WriteConcern writeConcern) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.serializer = serializer;
        this.writeConcern = writeConcern;
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


    // TODO: Revisit this
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

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    private BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    private <T> MongoReplyMessage<MongoDocument> sendWriteMessage(final MongoNamespace namespace,
                                                                  final MongoRequestMessage writeMessage,
                                                                  final MongoWrite write) {
        try {
            channel.sendMessage(writeMessage);
            if (write.getWriteConcern().callGetLastError()) {
                MongoQueryMessage getLastErrorMessage = new MongoQueryMessage(namespace.getFullName(),
                        new MongoQuery(writeConcern.getCommand()), new PooledByteBufferOutput(getBufferPool()),
                        serializer);
                channel.sendMessage(getLastErrorMessage);
                return channel.receiveMessage(serializer, MongoDocument.class);
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
                MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd",
                        new MongoQuery(command).batchSize(-1), new PooledByteBufferOutput(bufferPool), serializer);
                channel.sendMessage(message);

                MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializer, MongoDocument.class);

                return new CommandResult(replyMessage.getDocuments().get(0));
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, Class<T> clazz) {
            insert.setWriteConcernIfAbsent(writeConcern);
            MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                    clazz, new PooledByteBufferOutput(getBufferPool()), serializer);
            return new InsertResult(sendWriteMessage(namespace, insertMessage, insert));
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoQuery query, Class<T> clazz) {
            try {
                MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), query,
                        new PooledByteBufferOutput(bufferPool), serializer);
                channel.sendMessage(message);

                MongoReplyMessage<T> replyMessage = channel.receiveMessage(serializer, clazz);

                return new QueryResult<T>(replyMessage);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, GetMore getMore, Class<T> clazz) {
            try {
                MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore,
                        new PooledByteBufferOutput(bufferPool));
                channel.sendMessage(message);

                MongoReplyMessage<T> replyMessage = channel.receiveMessage(serializer, clazz);

                return new GetMoreResult<T>(replyMessage);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, MongoUpdate update) {
            update.setWriteConcernIfAbsent(writeConcern);
            MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update,
                    new PooledByteBufferOutput(bufferPool), serializer);
            return new UpdateResult(sendWriteMessage(namespace, message, update));
        }

        @Override
        public RemoveResult delete(final MongoNamespace namespace, MongoDelete delete) {
            delete.setWriteConcernIfAbsent(writeConcern);
            MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), delete,
                    new PooledByteBufferOutput(bufferPool), serializer);
            return new RemoveResult(sendWriteMessage(namespace, message, delete));
        }

        @Override
        public void killCursors(MongoKillCursor killCursor) {
            try {
                MongoKillCursorsMessage message = new MongoKillCursorsMessage(new PooledByteBufferOutput(bufferPool),
                        killCursor);
                channel.sendMessage(message);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }
    }
}

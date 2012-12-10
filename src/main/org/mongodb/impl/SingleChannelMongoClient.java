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
import org.mongodb.MongoClient;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDocument;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
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
import java.util.concurrent.Callable;

// TODO: should this be a public class?
public class SingleChannelMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool;
    private final Serializer serializer;
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private MongoChannel channel;

    SingleChannelMongoClient(final SimplePool<MongoChannel> channelPool, final BufferPool<ByteBuffer> bufferPool,
                             final Serializer serializer, WriteConcern writeConcern, ReadPreference readPreference) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.serializer = serializer;
        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
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
    public void withConnection(final Runnable runnable) {
        runnable.run();
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws Exception {
        return callable.call();
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

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    private BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    private MongoReplyMessage<MongoDocument> sendWriteMessage(final MongoNamespace namespace,
                                                              final MongoRequestMessage writeMessage,
                                                              final MongoWrite write) {
        try {
            channel.sendMessage(writeMessage);
            if (write.getWriteConcern().callGetLastError()) {
                MongoQueryMessage getLastErrorMessage = new MongoQueryMessage(namespace.getFullName(),
                        new MongoFind(writeConcern.getCommand()).readPreference(ReadPreference.primary()),
                        new PooledByteBufferOutput(getBufferPool()), serializer);
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
        public CommandResult executeCommand(final String database, final MongoCommandOperation commandOperation) {
            try {
                commandOperation.readPreferenceIfAbsent(getReadPreference());
                MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd",
                        commandOperation, new PooledByteBufferOutput(bufferPool), serializer);
                channel.sendMessage(message);

                MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializer, MongoDocument.class);

                return new CommandResult(replyMessage.getDocuments().get(0));
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, Class<T> clazz) {
            insert.writeConcernIfAbsent(writeConcern);
            MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                    clazz, new PooledByteBufferOutput(getBufferPool()), serializer);
            return new InsertResult(sendWriteMessage(namespace, insertMessage, insert));
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find, Class<T> clazz) {
            try {
                find.readPreferenceIfAbsent(getReadPreference());
                MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find,
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
                // TODO: set read preference on getMore
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
            update.writeConcernIfAbsent(writeConcern);
            MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update,
                    new PooledByteBufferOutput(bufferPool), serializer);
            return new UpdateResult(sendWriteMessage(namespace, message, update));
        }

        @Override
        public RemoveResult delete(final MongoNamespace namespace, MongoRemove remove) {
            remove.writeConcernIfAbsent(writeConcern);
            MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove,
                    new PooledByteBufferOutput(bufferPool), serializer);
            return new RemoveResult(sendWriteMessage(namespace, message, remove));
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

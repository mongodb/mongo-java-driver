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
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
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
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;
import org.mongodb.util.pool.SimplePool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

// TODO: should this be a public class?
public class SingleChannelMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool;
    private final PrimitiveSerializers primitiveSerializers;
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private MongoChannel channel;

    SingleChannelMongoClient(final SimplePool<MongoChannel> channelPool, final BufferPool<ByteBuffer> bufferPool,
                             final PrimitiveSerializers primitiveSerializers, final WriteConcern writeConcern,
                             final ReadPreference readPreference) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.primitiveSerializers = primitiveSerializers;
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
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
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

    @Override
    public PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    private BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }


    private Serializer<MongoDocument> withDocumentSerializer(final Serializer<MongoDocument> serializer) {
        if (serializer != null) {
            return serializer;
        }
        return new MongoDocumentSerializer(this.primitiveSerializers);
    }

    private MongoReplyMessage<MongoDocument> sendWriteMessage(final MongoNamespace namespace,
                                                              final MongoRequestMessage writeMessage,
                                                              final MongoWrite write,
                                                              final Serializer<MongoDocument> serializer) {
        try {
            channel.sendMessage(writeMessage);
            if (write.getWriteConcern().callGetLastError()) {
                final MongoQueryMessage getLastErrorMessage = new MongoQueryMessage(namespace.getFullName(),
                        new MongoFind(writeConcern.getCommand()).readPreference(ReadPreference.primary()),
                        new PooledByteBufferOutput(getBufferPool()), withDocumentSerializer(null));
                channel.sendMessage(getLastErrorMessage);
                return channel.receiveMessage(serializer);
            }
            else {
                return null;
            }
        } catch (IOException e) {
            throw new MongoException("insert", e);
        }
    }

    private class SingleChannelMongoOperations implements MongoOperations {
        @Override
        public MongoDocument executeCommand(final String database, final MongoCommandOperation commandOperation,
                                            final Serializer<MongoDocument> serializer) {
            try {
                commandOperation.readPreferenceIfAbsent(getReadPreference());
                final MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd",
                        commandOperation, new PooledByteBufferOutput(bufferPool), withDocumentSerializer(serializer));
                channel.sendMessage(message);

                // TODO: not sure about the serializer we're passing in here
                final MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializer);

                return replyMessage.getDocuments().get(0);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer) {
            insert.writeConcernIfAbsent(writeConcern);
            final MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                    new PooledByteBufferOutput(getBufferPool()), serializer);
            return new InsertResult(sendWriteMessage(namespace, insertMessage, insert, withDocumentSerializer(null)));
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                        final Serializer<MongoDocument> baseSerializer,
                                        final Serializer<T> serializer) {
            try {
                find.readPreferenceIfAbsent(getReadPreference());
                final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find,
                        new PooledByteBufferOutput(bufferPool), withDocumentSerializer(baseSerializer));
                channel.sendMessage(message);

                final MongoReplyMessage<T> replyMessage = channel.receiveMessage(serializer);

                return new QueryResult<T>(replyMessage);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, final GetMore getMore, final Serializer<T> serializer) {
            try {
                // TODO: set read preference on getMore
                final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore,
                        new PooledByteBufferOutput(bufferPool));
                channel.sendMessage(message);

                final MongoReplyMessage<T> replyMessage = channel.receiveMessage(serializer);

                return new GetMoreResult<T>(replyMessage);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, final MongoUpdate update, final Serializer<MongoDocument> serializer) {
            update.writeConcernIfAbsent(writeConcern);
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update,
                    new PooledByteBufferOutput(bufferPool), withDocumentSerializer(serializer));
            return new UpdateResult(sendWriteMessage(namespace, message, update, withDocumentSerializer(null)));
        }

        @Override
        public RemoveResult remove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<MongoDocument> serializer) {
            remove.writeConcernIfAbsent(writeConcern);
            final MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove,
                    new PooledByteBufferOutput(bufferPool), withDocumentSerializer(serializer));
            return new RemoveResult(sendWriteMessage(namespace, message, remove, withDocumentSerializer(null)));
        }

        @Override
        public void killCursors(final MongoKillCursor killCursor) {
            try {
                final MongoKillCursorsMessage message = new MongoKillCursorsMessage(new PooledByteBufferOutput(bufferPool),
                        killCursor);
                channel.sendMessage(message);
            } catch (IOException e) {
                throw new MongoException("", e);
            }
        }
    }
}

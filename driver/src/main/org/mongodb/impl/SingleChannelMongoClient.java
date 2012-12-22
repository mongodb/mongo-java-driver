/*
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
 */

package org.mongodb.impl;

import org.bson.io.PooledByteBufferOutput;
import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.ClientAdmin;
import org.mongodb.MongoClient;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.command.GetLastErrorCommand;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
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
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;
import org.mongodb.util.pool.SimplePool;

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
    public MongoDatabase getDatabase(final String databaseName) {
        return new MongoDatabaseImpl(databaseName, this);
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

    @Override
    public ClientAdmin admin() {
        //TODO - when will this be used via this class
        throw new IllegalStateException("Not implemented yet!");
    }

    private BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }


    private Serializer<Document> withDocumentSerializer(final Serializer<Document> serializer) {
        if (serializer != null) {
            return serializer;
        }
        return new DocumentSerializer(this.primitiveSerializers);
    }

    private CommandResult sendWriteMessage(final MongoNamespace namespace, final MongoRequestMessage writeMessage,
                                           final MongoWrite write) {
        channel.sendOneWayMessage(writeMessage);
        if (write.getWriteConcern().callGetLastError()) {
            return new GetLastErrorCommand(getDatabase(namespace.getDatabaseName()), write.getWriteConcern()).execute();
        }
        else {
            return null;
        }
    }

    private class SingleChannelMongoOperations implements MongoOperations {
        @Override
        public CommandResult executeCommand(final String database, final MongoCommandOperation commandOperation,
                                            final Serializer<Document> serializer) {
            commandOperation.readPreferenceIfAbsent(getReadPreference());
            final MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd", commandOperation,
                                                                    new PooledByteBufferOutput(bufferPool),
                                                                    withDocumentSerializer(serializer));
            // TODO: not sure about the serializer we're passing in here
            final MongoReplyMessage<Document> replyMessage = channel.sendQueryMessage(message, serializer);

            return new CommandResult(commandOperation.getCommand().toDocument(), channel.getAddress(),
                                     replyMessage.getDocuments().get(0));
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                       final Serializer<T> serializer) {
            insert.writeConcernIfAbsent(writeConcern);
            final MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                                                                                  new PooledByteBufferOutput(
                                                                                          getBufferPool()), serializer);
            return new InsertResult(insert, sendWriteMessage(namespace, insertMessage, insert));
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            find.readPreferenceIfAbsent(getReadPreference());
            final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find,
                                                                    new PooledByteBufferOutput(bufferPool),
                                                                    withDocumentSerializer(baseSerializer));
            return new QueryResult<T>(channel.sendQueryMessage(message, serializer));
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                            final Serializer<T> serializer) {
            final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore,
                                                                        new PooledByteBufferOutput(bufferPool));
            return new GetMoreResult<T>(channel.sendGetMoreMessage(message, serializer));
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, final MongoUpdate update,
                                   final Serializer<Document> serializer) {
            update.writeConcernIfAbsent(writeConcern);
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update,
                                                                      new PooledByteBufferOutput(bufferPool),
                                                                      withDocumentSerializer(serializer));
            return new UpdateResult(update, sendWriteMessage(namespace, message, update));
        }

        @Override
        public <T> UpdateResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            replace.writeConcernIfAbsent(writeConcern);
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), replace,
                                                                      new PooledByteBufferOutput(bufferPool),
                                                                      withDocumentSerializer(baseSerializer),
                                                                      serializer);
            return new UpdateResult(replace, sendWriteMessage(namespace, message, replace));
        }

        @Override
        public RemoveResult remove(final MongoNamespace namespace, final MongoRemove remove,
                                   final Serializer<Document> serializer) {
            remove.writeConcernIfAbsent(writeConcern);
            final MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove,
                                                                      new PooledByteBufferOutput(bufferPool),
                                                                      withDocumentSerializer(serializer));
            return new RemoveResult(remove, sendWriteMessage(namespace, message, remove));
        }

        @Override
        public void killCursors(final MongoKillCursor killCursor) {
            final MongoKillCursorsMessage message = new MongoKillCursorsMessage(new PooledByteBufferOutput(bufferPool),
                                                                                killCursor);
            channel.sendOneWayMessage(message);
        }
    }
}

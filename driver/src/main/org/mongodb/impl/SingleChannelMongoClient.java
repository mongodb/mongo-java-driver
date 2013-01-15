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

import org.mongodb.command.GetLastError;
import org.mongodb.io.PooledByteBufferOutput;
import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.ClientAdmin;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.operation.MongoWrite;
import org.mongodb.pool.SimplePool;
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
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

// TODO: this should not be a public class.  It is now because driver-compat needs it.
public class SingleChannelMongoClient implements MongoClient {

    private final SimplePool<MongoChannel> channelPool;
    private final BufferPool<ByteBuffer> bufferPool;
    private MongoChannel channel;
    private final MongoClientOptions options;

    SingleChannelMongoClient(final SimplePool<MongoChannel> channelPool, final BufferPool<ByteBuffer> bufferPool,
                             final MongoClientOptions options) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.options = options;
        this.channel = channelPool.get();
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions databaseOptions) {
        return new MongoDatabaseImpl(databaseName, this, databaseOptions.withDefaults(options));
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
    public MongoClientOptions getOptions() {
        return options;
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
        return new DocumentSerializer(options.getPrimitiveSerializers());
    }

    private CommandResult sendWriteMessage(final MongoNamespace namespace, final MongoRequestMessage writeMessage,
                                           final MongoWrite write) {
        channel.sendOneWayMessage(writeMessage);
        if (write.getWriteConcern().callGetLastError()) {
            return getLastError(namespace, write);
        }
        else {
            return null;
        }
    }

    private CommandResult getLastError(final MongoNamespace namespace, final MongoWrite write) {
        final GetLastError getLastError = new GetLastError(write.getWriteConcern());

        final CommandResult commandResult = getDatabase(namespace.getDatabaseName()).executeCommand(getLastError);
        return getLastError.parseGetLastErrorResponse(commandResult);
    }

    private class SingleChannelMongoOperations implements MongoOperations {
        @Override
        public CommandResult executeCommand(final String database, final MongoCommand commandOperation,
                                            final Serializer<Document> serializer) {
            commandOperation.readPreferenceIfAbsent(options.getReadPreference());
            final MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd", commandOperation,
                                                                    new PooledByteBufferOutput(bufferPool),
                                                                    withDocumentSerializer(serializer));
            final MongoReplyMessage<Document> replyMessage = channel.sendQueryMessage(message, serializer);

            return new CommandResult(commandOperation.toDocument(), channel.getAddress(),
                                     replyMessage.getDocuments().get(0));
        }

        @Override
        public <T> InsertResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                       final Serializer<T> serializer) {
            insert.writeConcernIfAbsent(options.getWriteConcern());
            final MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                                                                                  new PooledByteBufferOutput(
                                                                                          getBufferPool()), serializer);
            return new InsertResult(insert, sendWriteMessage(namespace, insertMessage, insert));
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            find.readPreferenceIfAbsent(options.getReadPreference());
            final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find,
                                                                    new PooledByteBufferOutput(bufferPool),
                                                                    withDocumentSerializer(baseSerializer));
            return new QueryResult<T>(channel.sendQueryMessage(message, serializer), channel.getAddress());
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                            final Serializer<T> serializer) {
            final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore,
                                                                        new PooledByteBufferOutput(bufferPool));
            return new GetMoreResult<T>(channel.sendGetMoreMessage(message, serializer), channel.getAddress());
        }

        @Override
        public UpdateResult update(final MongoNamespace namespace, final MongoUpdate update,
                                   final Serializer<Document> serializer) {
            update.writeConcernIfAbsent(options.getWriteConcern());
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update,
                                                                      new PooledByteBufferOutput(bufferPool),
                                                                      withDocumentSerializer(serializer));
            return new UpdateResult(update, sendWriteMessage(namespace, message, update));
        }

        @Override
        public <T> UpdateResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            replace.writeConcernIfAbsent(options.getWriteConcern());
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), replace,
                                                                      new PooledByteBufferOutput(bufferPool),
                                                                      withDocumentSerializer(baseSerializer),
                                                                      serializer);
            return new UpdateResult(replace, sendWriteMessage(namespace, message, replace));
        }

        @Override
        public RemoveResult remove(final MongoNamespace namespace, final MongoRemove remove,
                                   final Serializer<Document> serializer) {
            remove.writeConcernIfAbsent(options.getWriteConcern());
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

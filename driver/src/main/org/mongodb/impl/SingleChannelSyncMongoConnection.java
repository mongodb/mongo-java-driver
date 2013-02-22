/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.Document;
import org.mongodb.io.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.GetLastError;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.io.MongoChannel;
import org.mongodb.io.PooledByteBufferOutput;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
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
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

final class SingleChannelSyncMongoConnection implements MongoPoolableConnection {
    private final BufferPool<ByteBuffer> bufferPool;
    private final MongoClientOptions options;
    private final SimplePool<MongoPoolableConnection> channelPool;
    private MongoChannel channel;

    SingleChannelSyncMongoConnection(final ServerAddress serverAddress, final SimplePool<MongoPoolableConnection> channelPool,
                                     final BufferPool<ByteBuffer> bufferPool, final MongoClientOptions options) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.options = options;
        this.channel = new MongoChannel(serverAddress, bufferPool, new DocumentSerializer(options.getPrimitiveSerializers()));
    }

    @Override
    public CommandResult command(final String database, final MongoCommand commandOperation, final Serializer<Document> serializer) {
        commandOperation.readPreferenceIfAbsent(options.getReadPreference());
        final MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd", commandOperation,
                new PooledByteBufferOutput(bufferPool), withDocumentSerializer(serializer));
        final MongoReplyMessage<Document> replyMessage = channel.sendMessage(message, serializer);

        return createCommandResult(commandOperation, replyMessage);
    }

    private CommandResult createCommandResult(final MongoCommand commandOperation, final MongoReplyMessage<Document> replyMessage) {

        CommandResult commandResult = new CommandResult(commandOperation.toDocument(), channel.getAddress(),
                replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        find.readPreferenceIfAbsent(options.getReadPreference());
        final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find,
                new PooledByteBufferOutput(bufferPool), withDocumentSerializer(querySerializer));
        final MongoReplyMessage<T> replyMessage = channel.sendMessage(message, resultSerializer);
        return new QueryResult<T>(replyMessage, channel.getAddress());
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                      final Serializer<T> resultSerializer) {
        final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore,
                new PooledByteBufferOutput(bufferPool));
        final MongoReplyMessage<T> replyMessage = channel.sendMessage(message, resultSerializer);
        return new QueryResult<T>(replyMessage, channel.getAddress());
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                  final Serializer<T> serializer) {
        insert.writeConcernIfAbsent(options.getWriteConcern());
        final MongoInsertMessage<T> insertMessage = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                new PooledByteBufferOutput(bufferPool), serializer);
        return new WriteResult(insert, sendWriteMessage(namespace, insertMessage, insert.getWriteConcern()));
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update,
                              final Serializer<Document> querySerializer) {
        update.writeConcernIfAbsent(options.getWriteConcern());
        final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), update,
                new PooledByteBufferOutput(bufferPool), withDocumentSerializer(querySerializer));
        return new WriteResult(update, sendWriteMessage(namespace, message, update.getWriteConcern()));
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Serializer<Document> querySerializer, final Serializer<T> serializer) {
        replace.writeConcernIfAbsent(options.getWriteConcern());
        final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), replace,
                new PooledByteBufferOutput(bufferPool), withDocumentSerializer(querySerializer), serializer);
        return new WriteResult(replace, sendWriteMessage(namespace, message, replace.getWriteConcern()));
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
                              final Serializer<Document> querySerializer) {
        remove.writeConcernIfAbsent(options.getWriteConcern());
        final MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove,
                new PooledByteBufferOutput(bufferPool), withDocumentSerializer(querySerializer));
        return new WriteResult(remove, sendWriteMessage(namespace, message, remove.getWriteConcern()));
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        final MongoKillCursorsMessage message = new MongoKillCursorsMessage(new PooledByteBufferOutput(bufferPool), killCursor);
        channel.sendMessage(message);
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public void release() {
        if (channel == null) {
            throw new IllegalStateException("Can not release a channel that's already closed");
        }
        if (channelPool == null) {
            throw new IllegalStateException("Can not release a channel not associated with a pool");
        }

        channelPool.done(this);
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return Arrays.asList(channel.getAddress());
    }

    private Serializer<Document> withDocumentSerializer(final Serializer<Document> serializer) {
        if (serializer != null) {
            return serializer;
        }
        return new DocumentSerializer(options.getPrimitiveSerializers());
    }

    private CommandResult sendWriteMessage(final MongoNamespace namespace, final MongoRequestMessage writeMessage,
                                           final WriteConcern writeConcern) {
        channel.sendMessage(writeMessage);
        if (writeConcern.callGetLastError()) {
            return getLastError(namespace, writeConcern);
        }
        else {
            return null;
        }
    }

    private CommandResult getLastError(final MongoNamespace namespace, final WriteConcern writeConcern) {
        final GetLastError getLastError = new GetLastError(writeConcern);

        final DocumentSerializer serializer = new DocumentSerializer(options.getPrimitiveSerializers());
        final CommandResult commandResult = command(namespace.getDatabaseName(), getLastError, serializer);
        return getLastError.parseGetLastErrorResponse(commandResult);
    }

    @Override
    public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                              final Serializer<Document> serializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncCommand(final String database, final MongoCommand commandOperation,
                             final Serializer<Document> serializer, final SingleResultCallback<CommandResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Serializer<Document> querySerializer,
                               final Serializer<T> resultSerializer, final SingleResultCallback<QueryResult<T>> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                   final Serializer<T> resultSerializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore, final Serializer<T> resultSerializer,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Serializer<T> serializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer,
                                final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Serializer<Document> querySerializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update, final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Serializer<Document> querySerializer, final Serializer<T> serializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Serializer<Document> querySerializer, final Serializer<T> serializer,
                                 final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Serializer<Document> querySerializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<Document> querySerializer,
                            final SingleResultCallback<WriteResult> callback) {
        throw new UnsupportedOperationException();
    }
}

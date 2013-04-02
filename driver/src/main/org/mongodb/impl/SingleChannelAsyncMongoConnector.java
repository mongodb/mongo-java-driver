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
 */

package org.mongodb.impl;

import org.mongodb.Document;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.io.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.GetLastError;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.io.async.MongoAsynchronousSocketChannelGateway;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.operation.MongoWrite;
import org.mongodb.pool.SimplePool;
import org.mongodb.protocol.MongoCommandMessage;
import org.mongodb.protocol.MongoDeleteMessage;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoKillCursorsMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplaceMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.protocol.MongoUpdateMessage;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SingleChannelAsyncMongoConnector implements MongoPoolableConnector {
    private final ServerAddress serverAddress;
    private final SimplePool<MongoPoolableConnector> channelPool;
    private final BufferPool<ByteBuffer> bufferPool;
    private final MongoClientOptions options;
    private volatile MongoAsynchronousSocketChannelGateway channel;
    private volatile boolean activeAsyncCall;
    private volatile boolean releasePending;

    SingleChannelAsyncMongoConnector(final ServerAddress serverAddress, final SimplePool<MongoPoolableConnector> channelPool,
                                     final BufferPool<ByteBuffer> bufferPool, final MongoClientOptions options) {
        this.serverAddress = serverAddress;
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.options = options;
        this.channel = new MongoAsynchronousSocketChannelGateway(serverAddress, bufferPool);
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public synchronized void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public synchronized void release() {
        if (channel == null) {
            throw new IllegalStateException("Can not release a channel that's already closed");
        }
        if (channelPool == null) {
            throw new IllegalStateException("Can not release a channel not associated with a pool");
        }

        if (activeAsyncCall) {
            releasePending = true;
        }
        else {
            releasePending = false;
            channelPool.done(this);
        }
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

    private synchronized void releaseIfPending() {
        activeAsyncCall = false;
        if (releasePending) {
            release();
        }
    }

    @Override
    public CommandResult command(final String database, final MongoCommand commandOperation,
                                 final Serializer<Document> serializer) {
        try {
            CommandResult result = asyncCommand(database, commandOperation, serializer).get();
            if (result == null) {
                throw new MongoInternalException("Command result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                  final Serializer<T> serializer) {
        try {
            WriteResult result = asyncInsert(namespace, insert, serializer).get();
            if (result == null) {
                throw new MongoInternalException("Command result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        try {
            QueryResult<T> result = asyncQuery(namespace, find, querySerializer, resultSerializer).get();
            if (result == null) {
                throw new MongoInternalException("Query result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final MongoGetMore mongoGetMore,
                                      final Serializer<T> resultSerializer) {
        try {
            QueryResult<T> result = asyncGetMore(namespace, mongoGetMore, resultSerializer).get();
            if (result == null) {
                throw new MongoInternalException("Query result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Serializer<Document> querySerializer) {
        try {
            WriteResult result = asyncUpdate(namespace, update, querySerializer).get();
            if (result == null) {
                throw new MongoInternalException("Command result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Serializer<Document> querySerializer, final Serializer<T> serializer) {

        try {
            WriteResult result = asyncReplace(namespace, replace, querySerializer, serializer).get();
            if (result == null) {
                throw new MongoInternalException("Command result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
                              final Serializer<Document> querySerializer) {
        try {
            WriteResult result = asyncRemove(namespace, remove, querySerializer).get();
            if (result == null) {
                throw new MongoInternalException("Command result should not be null", null);
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("", e);
        } catch (ExecutionException e) {
            handleExecutionException(e);
        }
        throw new IllegalStateException("Should be unreachable");
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        final MongoKillCursorsMessage message = new MongoKillCursorsMessage(killCursor);
        serializeMessageToBuffer(message, buffer);
        channel.sendMessage(buffer);
    }

    @Override
    public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                              final Serializer<Document> serializer) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        asyncCommand(database, commandOperation, serializer, new SingleResultFutureCallback<CommandResult>(retVal));

        return retVal;
    }

    @Override
    public void asyncCommand(final String database, final MongoCommand commandOperation, final Serializer<Document> serializer,
                             final SingleResultCallback<CommandResult> callback) {
        commandOperation.readPreferenceIfAbsent(options.getReadPreference());
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        final MongoCommandMessage message = new MongoCommandMessage(database + ".$cmd", commandOperation,
                withDocumentSerializer(serializer));
        serializeMessageToBuffer(message, buffer);
        channel.sendAndReceiveMessage(buffer, new MongoCommandResultCallback(callback, commandOperation, this,
                withDocumentSerializer(serializer)));
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        asyncQuery(namespace, find, querySerializer, resultSerializer, new SingleResultFutureCallback<QueryResult<T>>(retVal));

        return retVal;
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Serializer<Document> querySerializer,
                               final Serializer<T> resultSerializer, final SingleResultCallback<QueryResult<T>> callback) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find, withDocumentSerializer(querySerializer));
        serializeMessageToBuffer(message, buffer);
        channel.sendAndReceiveMessage(buffer, new MongoQueryResultCallback<T>(callback, this, resultSerializer));
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final MongoGetMore mongoGetMore,
                                                   final Serializer<T> resultSerializer) {
        final SingleResultFuture<QueryResult<T>> retVal = new SingleResultFuture<QueryResult<T>>();

        asyncGetMore(namespace, mongoGetMore, resultSerializer, new SingleResultFutureCallback<QueryResult<T>>(retVal));

        return retVal;
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final MongoGetMore mongoGetMore, final Serializer<T> resultSerializer,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), mongoGetMore);
        serializeMessageToBuffer(message, buffer);
        channel.sendAndReceiveMessage(buffer, new MongoGetMoreResultCallback<T>(callback, this, resultSerializer,
                mongoGetMore.getServerCursor().getId()));
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Serializer<T> serializer) {
        final SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        asyncInsert(namespace, insert, serializer, new SingleResultFutureCallback<WriteResult>(retVal));

        return retVal;
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer,
                                final SingleResultCallback<WriteResult> callback) {
        insert.writeConcernIfAbsent(options.getWriteConcern());
        final MongoInsertMessage<T> message = new MongoInsertMessage<T>(namespace.getFullName(), insert, serializer);
        sendAsyncWriteMessage(namespace, insert, withDocumentSerializer(null), callback, message);
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Serializer<Document> querySerializer) {
        final SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        asyncUpdate(namespace, update, querySerializer, new SingleResultFutureCallback<WriteResult>(retVal));

        return retVal;
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate replace, final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        replace.writeConcernIfAbsent(options.getWriteConcern());
        final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), replace, withDocumentSerializer(serializer));
        sendAsyncWriteMessage(namespace, replace, serializer, callback, message);
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Serializer<Document> querySerializer, final Serializer<T> serializer) {
        final SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        asyncReplace(namespace, replace, querySerializer, serializer, new SingleResultFutureCallback<WriteResult>(retVal));

        return retVal;
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Serializer<Document> querySerializer, final Serializer<T> serializer,
                                 final SingleResultCallback<WriteResult> callback) {
        replace.writeConcernIfAbsent(options.getWriteConcern());
        final MongoReplaceMessage<T> message = new MongoReplaceMessage<T>(namespace.getFullName(), replace,
                withDocumentSerializer(querySerializer), serializer);
        sendAsyncWriteMessage(namespace, replace, querySerializer, callback, message);
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Serializer<Document> querySerializer) {
        final SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        asyncRemove(namespace, remove, querySerializer, new SingleResultFutureCallback<WriteResult>(retVal));

        return retVal;
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<Document> querySerializer,
                            final SingleResultCallback<WriteResult> callback) {
        remove.writeConcernIfAbsent(options.getWriteConcern());
        final MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove, withDocumentSerializer(querySerializer));
        sendAsyncWriteMessage(namespace, remove, querySerializer, callback, message);
    }

    private void sendAsyncWriteMessage(final MongoNamespace namespace, final MongoWrite write, final Serializer<Document> serializer,
                                       final SingleResultCallback<WriteResult> callback, final MongoRequestMessage message) {

        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferPool);
        serializeMessageToBuffer(message, buffer);
        if (write.getWriteConcern().callGetLastError()) {
            final GetLastError getLastError = new GetLastError(write.getWriteConcern());
            MongoCommandMessage getLastErrorMessage =
                    new MongoCommandMessage(namespace.getDatabaseName() + ".$cmd", getLastError, withDocumentSerializer(serializer));
            serializeMessageToBuffer(getLastErrorMessage, buffer);
            channel.sendAndReceiveMessage(buffer, new MongoWriteResultCallback(callback, write, getLastError, this,
                    withDocumentSerializer(serializer)));
        }
        else {
            channel.sendMessage(buffer, new MongoWriteResultCallback(callback, write, null, this, withDocumentSerializer(serializer)));
        }
    }

    private void serializeMessageToBuffer(final MongoRequestMessage message, final PooledByteBufferOutputBuffer buffer) {
        try {
            message.serialize(buffer);
        } catch (RuntimeException e) {
            buffer.close();
            throw e;
        } catch (Error e) {
            buffer.close();
            throw e;
        }
    }

    private void handleExecutionException(final ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
        }
        else if (e.getCause() instanceof Error) {
            throw (Error) e.getCause();
        }
        else {
            throw new MongoException("", e.getCause());
        }
    }


    private abstract static class MongoResponseCallback implements SingleResultCallback<ResponseBuffers> {
        private final SingleChannelAsyncMongoConnector connection;
        private volatile boolean closed;

        public MongoResponseCallback(final SingleChannelAsyncMongoConnector connection) {
            this.connection = connection;
            this.connection.activeAsyncCall = true;
        }

        @Override
        public void onResult(final ResponseBuffers responseBuffers, final MongoException e) {
            if (closed) {
                throw new MongoInternalException("This should not happen", null);
            }
            closed = true;
            final ServerAddress address = connection.getServerAddress();
            connection.releaseIfPending();
            if (responseBuffers != null) {
                callCallback(address, responseBuffers, e);
            }
            else {
                callCallback(address, null, e);
            }
        }

        public SingleChannelAsyncMongoConnector getConnection() {
            return connection;
        }

        protected abstract void callCallback(ServerAddress serverAddress, ResponseBuffers responseBuffers, MongoException e);
    }

    private static class MongoQueryResultCallback<T> extends MongoResponseCallback {
        private final SingleResultCallback<QueryResult<T>> callback;
        private final Serializer<T> serializer;

        public MongoQueryResultCallback(final SingleResultCallback<QueryResult<T>> callback,
                                        final SingleChannelAsyncMongoConnector connection,
                                        final Serializer<T> serializer) {
            super(connection);
            this.callback = callback;
            this.serializer = serializer;
        }

        @Override
        protected void callCallback(final ServerAddress serverAddress, final ResponseBuffers responseBuffers, final MongoException e) {
            try {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else if (responseBuffers.getReplyHeader().isQueryFailure()) {
                    final Document errorDocument = new MongoReplyMessage<Document>(responseBuffers,
                            getConnection().withDocumentSerializer(null)).getDocuments().get(0);
                    callback.onResult(null, new MongoQueryFailureException(getConnection().channel.getAddress(), errorDocument));
                }
                else {
                    callback.onResult(new QueryResult<T>(new MongoReplyMessage<T>(responseBuffers, serializer), serverAddress), null);
                }
            } finally {
                if (responseBuffers != null) {
                    responseBuffers.close();
                }
            }
        }
    }

    private static class MongoGetMoreResultCallback<T> extends MongoResponseCallback {
        private final SingleResultCallback<QueryResult<T>> callback;
        private final Serializer<T> serializer;
        private final long cursorId;

        public MongoGetMoreResultCallback(final SingleResultCallback<QueryResult<T>> callback,
                                          final SingleChannelAsyncMongoConnector connection,
                                          final Serializer<T> serializer,
                                          final long cursorId) {
            super(connection);
            this.callback = callback;
            this.serializer = serializer;
            this.cursorId = cursorId;
        }

        @Override
        protected void callCallback(final ServerAddress serverAddress, final ResponseBuffers responseBuffers, final MongoException e) {
            try {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                    callback.onResult(null, new MongoCursorNotFoundException(
                            new ServerCursor(cursorId, getConnection().channel.getAddress())));
                }
                else {
                    callback.onResult(new QueryResult<T>(new MongoReplyMessage<T>(responseBuffers, serializer), serverAddress), null);
                }
            } finally {
                if (responseBuffers != null) {
                    responseBuffers.close();
                }
            }
        }
    }

    private abstract static class MongoCommandResultBaseCallback extends MongoResponseCallback {
        private final MongoCommand commandOperation;
        private final Serializer<Document> serializer;

        public MongoCommandResultBaseCallback(final MongoCommand commandOperation,
                                              final SingleChannelAsyncMongoConnector client, final Serializer<Document> serializer) {
            super(client);
            this.commandOperation = commandOperation;
            this.serializer = serializer;
        }

        protected void callCallback(final ServerAddress serverAddress, final ResponseBuffers responseBuffers,
                                    final MongoException e) {
            try {
            if (responseBuffers != null) {
                MongoReplyMessage<Document> replyMessage = new MongoReplyMessage<Document>(responseBuffers, serializer);
                callCallback(new CommandResult(commandOperation.toDocument(), serverAddress,
                        replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds()), e);
            }
            else {
                callCallback(null, e);
            }
            } finally {
                if (responseBuffers != null) {
                    responseBuffers.close();
                }
            }
        }

        protected abstract void callCallback(final CommandResult commandResult, final MongoException e);
    }

    private static class MongoCommandResultCallback extends MongoCommandResultBaseCallback {
        private final SingleResultCallback<CommandResult> callback;

        public MongoCommandResultCallback(final SingleResultCallback<CommandResult> callback,
                                          final MongoCommand commandOperation, final SingleChannelAsyncMongoConnector client,
                                          final Serializer<Document> serializer) {
            super(commandOperation, client, serializer);
            this.callback = callback;
        }

        @Override
        protected void callCallback(final CommandResult commandResult, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else if (!commandResult.isOk()) {
                callback.onResult(null, new MongoCommandFailureException(commandResult));
            }
            else {
                callback.onResult(commandResult, null);
            }
        }
    }


    private static class MongoWriteResultCallback extends MongoCommandResultBaseCallback {
        private final SingleResultCallback<WriteResult> callback;
        private final MongoWrite writeOperation;
        private final GetLastError getLastError;

        public MongoWriteResultCallback(final SingleResultCallback<WriteResult> callback,
                                        final MongoWrite writeOperation, final GetLastError getLastError,
                                        final SingleChannelAsyncMongoConnector client,
                                        final Serializer<Document> serializer) {
            super(getLastError, client, serializer);
            this.callback = callback;
            this.writeOperation = writeOperation;
            this.getLastError = getLastError;
        }

        @Override
        protected void callCallback(final CommandResult commandResult, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else if (getLastError != null && !commandResult.isOk()) {
                callback.onResult(null, new MongoCommandFailureException(commandResult));
            }
            else {
                MongoCommandFailureException commandException = null;
                if (getLastError != null) {
                    commandException = getLastError.getCommandException(commandResult);
                }

                if (commandException != null) {
                    callback.onResult(null, commandException);
                }
                else {
                    callback.onResult(new WriteResult(writeOperation, commandResult), null);
                }
            }
        }
    }
}

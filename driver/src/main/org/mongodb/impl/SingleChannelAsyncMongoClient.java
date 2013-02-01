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

import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.ServerAddress;
import org.mongodb.async.MongoAsyncOperations;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.GetLastError;
import org.mongodb.command.MongoCommandException;
import org.mongodb.io.PooledByteBufferOutput;
import org.mongodb.io.async.MongoAsynchronousChannel;
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
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SingleChannelAsyncMongoClient extends SingleChannelMongoClient {
    private final SimplePool<MongoAsynchronousChannel> channelPool;
    private MongoAsynchronousChannel channel;
    private volatile boolean activeAsyncCall;
    private volatile boolean closePending;

    SingleChannelAsyncMongoClient(final ServerAddress serverAddress, final BufferPool<ByteBuffer> bufferPool,
                                  final MongoClientOptions options) {
        super(serverAddress, bufferPool, options);
        this.channelPool = null;
        this.channel = new MongoAsynchronousChannel(serverAddress, bufferPool, new DocumentSerializer(options.getPrimitiveSerializers()));
    }

    SingleChannelAsyncMongoClient(final ServerAddress serverAddress, final SimplePool<MongoAsynchronousChannel> channelPool,
                                  final BufferPool<ByteBuffer> bufferPool, final MongoClientOptions options) {
        super(serverAddress, bufferPool, options);
        this.channelPool = channelPool;
        this.channel = channelPool.get();
    }

    @Override
    public MongoOperations getOperations() {
        return new SingleChannelMongoOperations();
    }

    @Override
    public MongoAsyncOperations getAsyncOperations() {
        return new SingleChannelMongoOperations();
    }

    @Override
    public void close() {
        if (activeAsyncCall) {
            closePending = true;
            return;
        }
        if (channel == null) {
            return;
        }
        if (channelPool != null) {
            channelPool.done(channel);
        } else {
            channel.close();
        }
        channel = null;
        closePending = false;
    }

    private Serializer<Document> withDocumentSerializer(final Serializer<Document> serializer) {
        if (serializer != null) {
            return serializer;
        }
        return new DocumentSerializer(getOptions().getPrimitiveSerializers());
    }

    private void closeIfPending() {
        activeAsyncCall = false;
        if (closePending) {
            close();
        }
    }

    private class SingleChannelMongoOperations implements MongoOperations, MongoAsyncOperations {
        @Override
        public CommandResult executeCommand(final String database, final MongoCommand commandOperation,
                                            final Serializer<Document> serializer) {
            try {
                CommandResult result = asyncExecuteCommand(database, commandOperation, serializer).get();
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
                                      final Serializer<T> serializer, final Serializer<Document> baseSerializer) {
            try {
                WriteResult result = asyncInsert(namespace, insert, serializer, baseSerializer).get();
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
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            find.readPreferenceIfAbsent(getOptions().getReadPreference());
            final MongoQueryMessage message = new MongoQueryMessage(namespace.getFullName(), find,
                    new PooledByteBufferOutput(getBufferPool()),
                    withDocumentSerializer(baseSerializer));
            return new QueryResult<T>(channel.sendQueryMessage(message, serializer), channel.getAddress());
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                            final Serializer<T> serializer) {
            final MongoGetMoreMessage message = new MongoGetMoreMessage(namespace.getFullName(), getMore,
                    new PooledByteBufferOutput(getBufferPool()));
            return new GetMoreResult<T>(channel.sendGetMoreMessage(message, serializer), channel.getAddress());
        }

        @Override
        public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Serializer<Document> serializer) {
            try {
                WriteResult result = asyncUpdate(namespace, update, serializer).get();
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
                                       final Serializer<Document> baseSerializer, final Serializer<T> serializer) {

            try {
                WriteResult result = asyncReplace(namespace, replace, baseSerializer, serializer).get();
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
                                  final Serializer<Document> serializer) {
            try {
                WriteResult result = asyncRemove(namespace, remove, serializer).get();
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
            final MongoKillCursorsMessage message = new MongoKillCursorsMessage(new PooledByteBufferOutput(getBufferPool()),
                    killCursor);
            channel.sendMessage(message);
        }

        @Override
        public Future<CommandResult> asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                                         final Serializer<Document> serializer) {
            final ChannelFuture<CommandResult> retVal = new ChannelFuture<CommandResult>();

            asyncExecuteCommand(database, commandOperation, serializer, new ChannelSingleResultCallback<CommandResult>(retVal));

            return retVal;
        }

        @Override
        public void asyncExecuteCommand(final String database, final MongoCommand commandOperation, final Serializer<Document> serializer,
                                        final SingleResultCallback<CommandResult> callback) {
            commandOperation.readPreferenceIfAbsent(getOptions().getReadPreference());
            final MongoQueryMessage message = new MongoQueryMessage(database + ".$cmd", commandOperation,
                    new PooledByteBufferOutput(getBufferPool()),
                    withDocumentSerializer(serializer));
            channel.sendQueryMessage(message, serializer, new MongoReplyMessageCommandResultCallback(callback, commandOperation,
                    SingleChannelAsyncMongoClient.this));

        }

        @Override
        public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                                   final Serializer<T> serializer, final Serializer<Document> baseSerializer) {
            final ChannelFuture<WriteResult> retVal = new ChannelFuture<WriteResult>();

            asyncInsert(namespace, insert, serializer, baseSerializer, new ChannelSingleResultCallback<WriteResult>(retVal));

            return retVal;
        }

        @Override
        public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer,
                                    final Serializer<Document> baseSerializer, final SingleResultCallback<WriteResult> callback) {
            insert.writeConcernIfAbsent(getOptions().getWriteConcern());
            final MongoInsertMessage<T> message = new MongoInsertMessage<T>(namespace.getFullName(), insert,
                    new PooledByteBufferOutput(getBufferPool()), serializer);
            sendAsyncWriteMessage(namespace, insert, baseSerializer, callback, message);
        }

        @Override
        public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                               final Serializer<Document> serializer) {
            final ChannelFuture<WriteResult> retVal = new ChannelFuture<WriteResult>();

            asyncUpdate(namespace, update, serializer, new ChannelSingleResultCallback<WriteResult>(retVal));

            return retVal;
        }

        @Override
        public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate replace, final Serializer<Document> serializer,
                                final SingleResultCallback<WriteResult> callback) {
            replace.writeConcernIfAbsent(getOptions().getWriteConcern());
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), replace,
                    new PooledByteBufferOutput(getBufferPool()), withDocumentSerializer(serializer));
            sendAsyncWriteMessage(namespace, replace, serializer, callback, message);
        }

        @Override
        public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                    final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            final ChannelFuture<WriteResult> retVal = new ChannelFuture<WriteResult>();

            asyncReplace(namespace, replace, baseSerializer, serializer, new ChannelSingleResultCallback<WriteResult>(retVal));

            return retVal;
        }

        @Override
        public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                     final Serializer<Document> baseSerializer, final Serializer<T> serializer,
                                     final SingleResultCallback<WriteResult> callback) {
            replace.writeConcernIfAbsent(getOptions().getWriteConcern());
            final MongoUpdateMessage message = new MongoUpdateMessage(namespace.getFullName(), replace,
                    new PooledByteBufferOutput(getBufferPool()), withDocumentSerializer(baseSerializer), serializer);
            sendAsyncWriteMessage(namespace, replace, baseSerializer, callback, message);
        }

        @Override
        public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                               final Serializer<Document> serializer) {
            final ChannelFuture<WriteResult> retVal = new ChannelFuture<WriteResult>();

            asyncRemove(namespace, remove, serializer, new ChannelSingleResultCallback<WriteResult>(retVal));

            return retVal;
        }

        @Override
        public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<Document> serializer,
                                final SingleResultCallback<WriteResult> callback) {
            remove.writeConcernIfAbsent(getOptions().getWriteConcern());
            final MongoDeleteMessage message = new MongoDeleteMessage(namespace.getFullName(), remove,
                    new PooledByteBufferOutput(getBufferPool()),
                    withDocumentSerializer(serializer));
            sendAsyncWriteMessage(namespace, remove, serializer, callback, message);
        }

        private void sendAsyncWriteMessage(final MongoNamespace namespace, final MongoWrite write, final Serializer<Document> serializer,
                                           final SingleResultCallback<WriteResult> callback, final MongoRequestMessage message) {
            if (write.getWriteConcern().callGetLastError()) {
                final GetLastError getLastError = new GetLastError(write.getWriteConcern());
                final MongoQueryMessage writeConcernMessage = new MongoQueryMessage(namespace.getDatabaseName() + ".$cmd",
                        getLastError, new PooledByteBufferOutput(getBufferPool()),
                        withDocumentSerializer(serializer));
                channel.sendMessage(message, writeConcernMessage, serializer,
                        new MongoReplyMessageWriteResultCallback(callback, write, getLastError, SingleChannelAsyncMongoClient.this));
            } else {
                channel.sendMessage(message, new MongoReplyMessageWriteResultCallback(callback, write, null,
                        SingleChannelAsyncMongoClient.this));
            }
        }

        private void handleExecutionException(final ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new MongoException("", e.getCause());
            }
        }
    }

    private static class ChannelFuture<T> implements Future<T> {
        private T result;
        private MongoException exception;

        synchronized void init(final T newResult, final MongoException newException) {
            this.result = newResult;
            this.exception = newException;
            notifyAll();
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public synchronized boolean isDone() {
            return result != null || exception != null;
        }

        @Override
        public synchronized T get() throws InterruptedException, ExecutionException {
            while (!isDone()) {
                wait();
            }
            if (exception != null) {
                throw new ExecutionException(exception);
            }
            return result;
        }

        @Override
        public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (!isDone()) {
                wait(unit.toMillis(timeout));
            }
            if (!isDone()) {
                throw new TimeoutException(String.format("Timed out waiting for %d %s", timeout, unit));
            }
            if (exception != null) {
                throw new ExecutionException(exception);
            }
            return result;
        }
    }

    private static class ChannelSingleResultCallback<T> implements SingleResultCallback<T> {
        private final ChannelFuture<T> retVal;

        public ChannelSingleResultCallback(final ChannelFuture<T> retVal) {
            this.retVal = retVal;
        }

        @Override
        public void onResult(final T result, final MongoException e) {
            retVal.init(result, e);
        }
    }

    private abstract static class MongoReplyMessageResultCallback
            implements SingleResultCallback<org.mongodb.protocol.MongoReplyMessage<Document>> {
        private final MongoCommand commandOperation;
        private final SingleChannelAsyncMongoClient client;
        private volatile boolean closed;

        public MongoReplyMessageResultCallback(final MongoCommand commandOperation, final SingleChannelAsyncMongoClient client) {
            this.commandOperation = commandOperation;
            this.client = client;
            this.client.activeAsyncCall = true;
        }

        @Override
        public void onResult(final MongoReplyMessage<Document> replyMessage, final MongoException e) {
            if (closed) {
                throw new MongoInternalException("This should not happen", null);
            }
            closed = true;
            final ServerAddress address = client.getServerAddress();
            client.closeIfPending();
            if (replyMessage != null) {
                callCallback(new CommandResult(commandOperation.toDocument(), address,
                        replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds()), e);
            } else {
                callCallback(null, e);
            }
        }

        protected abstract void callCallback(final CommandResult commandResult, final MongoException e);
    }

    private static class MongoReplyMessageCommandResultCallback extends MongoReplyMessageResultCallback {
        private final SingleResultCallback<CommandResult> callback;

        public MongoReplyMessageCommandResultCallback(final SingleResultCallback<CommandResult> callback,
                                                      final MongoCommand commandOperation, final SingleChannelAsyncMongoClient client) {
            super(commandOperation, client);
            this.callback = callback;
        }

        @Override
        protected void callCallback(final CommandResult commandResult, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            } else {
                callback.onResult(commandResult, null);
            }
        }
    }


    private static class MongoReplyMessageWriteResultCallback extends MongoReplyMessageResultCallback {
        private final SingleResultCallback<WriteResult> callback;
        private final MongoWrite writeOperation;
        private final GetLastError getLastError;

        public MongoReplyMessageWriteResultCallback(final SingleResultCallback<WriteResult> callback,
                                                    final MongoWrite writeOperation, final GetLastError getLastError,
                                                    final SingleChannelAsyncMongoClient client) {
            super(getLastError, client);
            this.callback = callback;
            this.writeOperation = writeOperation;
            this.getLastError = getLastError;
        }

        @Override
        protected void callCallback(final CommandResult commandResult, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            } else {
                MongoCommandException commandException = null;
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

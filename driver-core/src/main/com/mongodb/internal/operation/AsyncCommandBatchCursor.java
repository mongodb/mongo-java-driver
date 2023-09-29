/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NEXT_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NO_OP_FIELD_NAME_VALIDATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getMoreCommandDocument;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.translateCommandException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

class AsyncCommandBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    @Nullable
    private final BsonValue comment;

    private final AtomicInteger count = new AtomicInteger();
    private final AtomicBoolean processedInitial = new AtomicBoolean();
    private final int maxWireVersion;
    private final boolean firstBatchEmpty;
    private final ResourceManager resourceManager;
    private volatile CommandCursorResult<T> commandCursorResult;
    private int batchSize;

    AsyncCommandBatchCursor(
            final ServerAddress serverAddress,
            final BsonDocument commandCursorDocument,
            final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder,
            @Nullable final BsonValue comment,
            final AsyncConnectionSource connectionSource,
            final AsyncConnection connection) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        CommandCursorResult<T> commandCursor = initFromCommandCursorDocument(serverAddress, FIRST_BATCH, commandCursorDocument);
        this.namespace = commandCursor.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.maxTimeMS = maxTimeMS;
        this.decoder = notNull("decoder", decoder);
        this.comment = comment;
        this.maxWireVersion = connection.getDescription().getMaxWireVersion();
        this.firstBatchEmpty = commandCursor.getResults().isEmpty();

        AsyncConnection connectionToPin = null;
        boolean releaseServerAndResources = false;
        if (limitReached()) {
            releaseServerAndResources = true;
        } else if (connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER) {
            connectionToPin = connection;
        }

        resourceManager = new ResourceManager(namespace, connectionSource, connectionToPin, commandCursor.getServerCursor());
        if (releaseServerAndResources) {
            resourceManager.releaseServerAndClientResources(connection);
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoException(MESSAGE_IF_CLOSED_AS_CURSOR));
            return;
        } else if (!resourceManager.tryStartOperation()) {
            callback.onResult(null, new MongoException(MESSAGE_IF_CONCURRENT_OPERATION));
            return;
        }

        ServerCursor localServerCursor = resourceManager.serverCursor();
        boolean cursorClosed = localServerCursor == null;
        List<T> batchResults = emptyList();
        if (!processedInitial.getAndSet(true) && !firstBatchEmpty) {
            batchResults = commandCursorResult.getResults();
        }

        if (cursorClosed || !batchResults.isEmpty()) {
            resourceManager.endOperation();
            if (cursorClosed) {
                close();
            }
            callback.onResult(batchResults, null);
        } else {
            getMore(localServerCursor, callback);
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public boolean isClosed() {
        return !resourceManager.operable();
    }

    @Override
    public void close() {
        resourceManager.close();
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return commandCursorResult.getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return commandCursorResult.getOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return firstBatchEmpty;
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    @Nullable
    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    ServerCursor getServerCursor() {
        return resourceManager.serverCursor();
    }

    private void getMore(final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        resourceManager.executeWithConnection(connection -> getMore(connection, cursor, callback), callback);
    }

    private void getMore(final AsyncConnection connection, final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        connection.commandAsync(namespace.getDatabaseName(),
                getMoreCommandDocument(cursor.getId(), connection.getDescription(), namespace,
                        limit, batchSize, count.get(), maxTimeMS, comment),
                NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(),
                CommandResultDocumentCodec.create(decoder, NEXT_BATCH), assertNotNull(resourceManager.connectionSource),
                (commandResult, t) -> {
                    if (t != null) {
                        Throwable translatedException =
                                    t instanceof MongoCommandException
                                            ? translateCommandException((MongoCommandException) t, cursor)
                                            : t;

                        connection.release();
                        resourceManager.endOperation();
                        callback.onResult(null, translatedException);
                        return;
                    }

                    CommandCursorResult<T> commandCursor =
                            initFromCommandCursorDocument(connection.getDescription().getServerAddress(), NEXT_BATCH, assertNotNull(commandResult));
                    resourceManager.setServerCursor(commandCursor.getServerCursor());

                    if (!resourceManager.operable()) {
                        resourceManager.releaseServerAndClientResources(connection);
                        connection.release();
                        callback.onResult(emptyList(), null);
                        return;
                    }

                    if (commandCursor.getResults().isEmpty() && commandCursor.getServerCursor() != null) {
                        connection.release();
                        getMore(commandCursor.getServerCursor(), callback);
                    } else {
                        resourceManager.endOperation();
                        if (limitReached()) {
                            resourceManager.releaseServerAndClientResources(connection);
                        }
                        connection.release();
                        callback.onResult(commandCursor.getResults(), null);
                    }
                });
    }

    private CommandCursorResult<T> initFromCommandCursorDocument(
            final ServerAddress serverAddress,
            final String fieldNameContainingBatch,
            final BsonDocument commandCursorDocument) {
        CommandCursorResult<T> cursorResult = new CommandCursorResult<>(serverAddress, fieldNameContainingBatch, commandCursorDocument);
        count.addAndGet(cursorResult.getResults().size());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Received batch of %d documents with cursorId %d from server %s", cursorResult.getResults().size(),
                    cursorResult.getCursorId(), cursorResult.getServerAddress()));
        }
        this.commandCursorResult = cursorResult;
        return cursorResult;
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count.get() >= Math.abs(limit);
    }

    @ThreadSafe
    private static final class ResourceManager extends CursorResourceManager<AsyncConnectionSource, AsyncConnection> {

        ResourceManager(
                final MongoNamespace namespace,
                final AsyncConnectionSource connectionSource,
                @Nullable final AsyncConnection connectionToPin,
                @Nullable  final ServerCursor serverCursor) {
            super(namespace, connectionSource, connectionToPin, serverCursor);
        }

        @Override
        void markAsPinned(final AsyncConnection connectionToPin, final Connection.PinningMode pinningMode) {
             connectionToPin.markAsPinned(pinningMode);
        }

        @Override
        void executeWithConnection(final Consumer<AsyncConnection> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        <R> void executeWithConnection(final Consumer<AsyncConnection> action, final SingleResultCallback<R> callback) {
            assertTrue(state != State.IDLE);
            if (pinnedConnection != null) {
                executeWithConnection(assertNotNull(pinnedConnection).retain(), null, action, callback);
            } else {
                assertNotNull(connectionSource).getConnection((conn, t) -> executeWithConnection(conn, t, action, callback));
            }
        }

        <R> void executeWithConnection(
                @Nullable final AsyncConnection connection,
                @Nullable final Throwable t,
                final Consumer<AsyncConnection> function,
                final SingleResultCallback<R> callback) {
            assertTrue(connection != null || t != null);
            if (t != null) {
                callback.onResult(null, t);
            } else {
                AsyncCallbackSupplier<R> curriedFunction = c -> function.accept(connection);
                curriedFunction.whenComplete(() -> {
                    System.out.println("COMPLETED!!!!");
                    connection.release();
                }).get((result, error) -> {
                    if (error instanceof MongoSocketException) {
                        onCorruptedConnection(connection);
                    }
                    callback.onResult(result, error);
                });
            }
        }

        @Override
        void doClose() {
            if (skipReleasingServerResourcesOnClose) {
                serverCursor = null;
            }

            if (serverCursor != null) {
                executeWithConnection(conn -> {
                    releaseServerResources(conn);
                    conn.release();
                }, (r, t) -> {
                    // guarantee that regardless of exceptions, `serverCursor` is null and client resources are released
                    serverCursor = null;
                    releaseClientResources();
                });
            } else {
                releaseClientResources();
            }
        }

        @Override
        void killServerCursor(final MongoNamespace namespace, final ServerCursor serverCursor, final AsyncConnection connection) {
            connection
                    .commandAsync(namespace.getDatabaseName(), getKillCursorsCommand(namespace, serverCursor),
                            NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(), assertNotNull(connectionSource),
                            (r, t) -> releaseClientResources());
        }
    }
}

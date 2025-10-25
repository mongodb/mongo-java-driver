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
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_ITERATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NEXT_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getKillCursorsCommand;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getMoreCommandDocument;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.logCommandCursorResult;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.translateCommandException;

class CommandCursor<T> implements Cursor<T> {

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    @Nullable
    private final BsonValue comment;
    private final int maxWireVersion;
    private final boolean firstBatchEmpty;
    private final ResourceManager resourceManager;

    private int batchSize;
    private CommandCursorResult<T> commandCursorResult;
    @Nullable
    private List<T> nextBatch;

    CommandCursor(
            final BsonDocument commandCursorDocument,
            final int batchSize,
            final Decoder<T> decoder,
            @Nullable final BsonValue comment,
            final ConnectionSource connectionSource,
            final Connection connection) {
        ConnectionDescription connectionDescription = connection.getDescription();
        this.commandCursorResult = toCommandCursorResult(connectionDescription.getServerAddress(), FIRST_BATCH, commandCursorDocument);
        this.namespace = commandCursorResult.getNamespace();
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.comment = comment;
        this.maxWireVersion = connectionDescription.getMaxWireVersion();
        this.firstBatchEmpty = commandCursorResult.getResults().isEmpty();

        Connection connectionToPin = connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER ? connection : null;
        resourceManager = new ResourceManager(namespace, connectionSource, connectionToPin, commandCursorResult.getServerCursor());
    }

    @Override
    public boolean hasNext(final OperationContext operationContext) {
        return assertNotNull(resourceManager.execute(MESSAGE_IF_CLOSED_AS_CURSOR, () -> doHasNext(operationContext), operationContext));
    }


    private boolean doHasNext(final OperationContext operationContext) {
        if (nextBatch != null) {
            return true;
        }

        while (resourceManager.getServerCursor() != null) {
            getMore(operationContext);
            if (!resourceManager.operable()) {
                throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_CURSOR);
            }
            if (nextBatch != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<T> next(final OperationContext operationContext) {
        return assertNotNull(resourceManager.execute(MESSAGE_IF_CLOSED_AS_ITERATOR, () -> doNext(operationContext), operationContext));
    }

    @Override
    public int available() {
        return !resourceManager.operable() || nextBatch == null ? 0 : nextBatch.size();
    }

    @Nullable
    private List<T> doNext(final OperationContext operationContext) {
        if (!doHasNext(operationContext)) {
            throw new NoSuchElementException();
        }

        List<T> retVal = nextBatch;
        nextBatch = null;
        commandCursorResult = CommandCursorResult.withEmptyResults(commandCursorResult);
        return retVal;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    boolean isClosed() {
        return !resourceManager.operable();
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
    public void close(final OperationContext operationContext) {
        resourceManager.close(operationContext);
    }

    @Nullable
    @Override
    public List<T> tryNext(final OperationContext operationContext) {
        return resourceManager.execute(MESSAGE_IF_CLOSED_AS_CURSOR, () -> {
            if (!tryHasNext(operationContext)) {
                return null;
            }
            return doNext(operationContext);
        }, operationContext);
    }

    private boolean tryHasNext(final OperationContext operationContext) {
        if (nextBatch != null) {
            return true;
        }

        if (resourceManager.getServerCursor() != null) {
            getMore(operationContext);
        }

        return nextBatch != null;
    }

    @Override
    @Nullable
    public ServerCursor getServerCursor() {
        if (!resourceManager.operable()) {
            throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_ITERATOR);
        }
        return resourceManager.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        if (!resourceManager.operable()) {
            throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_ITERATOR);
        }

        return commandCursorResult.getServerAddress();
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

    private void getMore(final OperationContext operationContext) {
        ServerCursor serverCursor = assertNotNull(resourceManager.getServerCursor());
        resourceManager.executeWithConnection(connection -> {
            ServerCursor nextServerCursor;
            try {
                this.commandCursorResult = toCommandCursorResult(connection.getDescription().getServerAddress(), NEXT_BATCH,
                        assertNotNull(
                                connection.command(namespace.getDatabaseName(),
                                        getMoreCommandDocument(serverCursor.getId(), connection.getDescription(), namespace, batchSize,
                                                comment),
                                        NoOpFieldNameValidator.INSTANCE,
                                        ReadPreference.primary(),
                                        CommandResultDocumentCodec.create(decoder, NEXT_BATCH),
                                        operationContext)));
                nextServerCursor = commandCursorResult.getServerCursor();
            } catch (MongoCommandException e) {
                throw translateCommandException(e, serverCursor);
            }
            resourceManager.setServerCursor(nextServerCursor);
        }, operationContext);
    }

    private CommandCursorResult<T> toCommandCursorResult(final ServerAddress serverAddress, final String fieldNameContainingBatch,
                                                         final BsonDocument commandCursorDocument) {
        CommandCursorResult<T> commandCursorResult = new CommandCursorResult<>(serverAddress, fieldNameContainingBatch,
                commandCursorDocument);
        logCommandCursorResult(commandCursorResult);
        this.nextBatch = commandCursorResult.getResults().isEmpty() ? null : commandCursorResult.getResults();
        return commandCursorResult;
    }


    @ThreadSafe
    private final class ResourceManager extends CursorResourceManager<ConnectionSource, Connection> {
        ResourceManager(
                final MongoNamespace namespace,
                final ConnectionSource connectionSource,
                @Nullable final Connection connectionToPin,
                @Nullable final ServerCursor serverCursor) {
            super(namespace, connectionSource, connectionToPin, serverCursor);
        }

        /**
         * Thread-safe.
         */
        @Nullable
        <R> R execute(final String exceptionMessageIfClosed, final Supplier<R> operation, final OperationContext operationContext)
                throws IllegalStateException {
            if (!tryStartOperation()) {
                throw new IllegalStateException(exceptionMessageIfClosed);
            }
            try {
                return operation.get();
            } finally {
                endOperation(operationContext);
            }
        }

        @Override
        void markAsPinned(final Connection connectionToPin, final Connection.PinningMode pinningMode) {
            connectionToPin.markAsPinned(pinningMode);
        }

        @Override
        void doClose(final OperationContext operationContext) {
                releaseResources(operationContext);
        }

        private void releaseResources(final OperationContext operationContext) {
            try {
                if (isSkipReleasingServerResourcesOnClose()) {
                    unsetServerCursor();
                }
                if (super.getServerCursor() != null) {
                    Connection connection = getConnection(operationContext);
                    try {
                        releaseServerResources(connection, operationContext);
                    } finally {
                        connection.release();
                    }
                }
            } catch (MongoException e) {
                // ignore exceptions when releasing server resources
            } finally {
                // guarantee that regardless of exceptions, `serverCursor` is null and client resources are released
                unsetServerCursor();
                releaseClientResources();
            }
        }

        void executeWithConnection(final Consumer<Connection> action, final OperationContext operationContext) {
            Connection connection = getConnection(operationContext);
            try {
                action.accept(connection);
            } catch (MongoSocketException e) {
                onCorruptedConnection(connection, e);
                throw e;
            } catch (MongoOperationTimeoutException e) {
                Throwable cause = e.getCause();
                if (cause instanceof MongoSocketException) {
                    onCorruptedConnection(connection, (MongoSocketException) cause);
                }
                throw e;
            } finally {
                connection.release();
            }
        }

        private Connection getConnection(final OperationContext operationContext) {
            assertTrue(getState() != State.IDLE);
            Connection pinnedConnection = getPinnedConnection();
            if (pinnedConnection == null) {
                return assertNotNull(getConnectionSource()).getConnection(operationContext);
            } else {
                return pinnedConnection.retain();
            }
        }

        private void releaseServerResources(final Connection connection, final OperationContext operationContext) {
            try {
                ServerCursor localServerCursor = super.getServerCursor();
                if (localServerCursor != null) {
                    killServerCursor(getNamespace(), localServerCursor, connection, operationContext);
                }
            } finally {
                unsetServerCursor();
            }
        }

        private void killServerCursor(
                final MongoNamespace namespace,
                final ServerCursor localServerCursor,
                final Connection localConnection,
                final OperationContext operationContext) {
            localConnection.command(
                    namespace.getDatabaseName(),
                    getKillCursorsCommand(namespace, localServerCursor),
                    NoOpFieldNameValidator.INSTANCE,
                    ReadPreference.primary(),
                    new BsonDocumentCodec(),
                    operationContext);
        }
    }
}

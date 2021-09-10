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

import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackFunction;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.async.function.AsyncCallbackBiFunction;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.NonNull;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotTwo;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotSix;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotTwo;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

final class OperationHelper {
    public static final Logger LOGGER = Loggers.getLogger("operation");

    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithSource<T> {
        T call(ConnectionSource source);
    }

    interface AsyncCallableWithConnection {
        void call(AsyncConnection connection, Throwable t);
    }

    interface AsyncCallableWithSource {
        void call(AsyncConnectionSource source, Throwable t);
    }

    interface AsyncCallableWithConnectionAndSource {
        void call(AsyncConnectionSource source, AsyncConnection connection, Throwable t);
    }

    static void validateReadConcern(final Connection connection, final ReadConcern readConcern) {
        validateReadConcern(connection.getDescription(), readConcern);
    }

    static void validateReadConcern(final ConnectionDescription description, final ReadConcern readConcern) {
        if (!serverIsAtLeastVersionThreeDotTwo(description) && !readConcern.isServerDefault()) {
            throw new IllegalArgumentException(format("ReadConcern not supported by wire version: %s",
                    description.getMaxWireVersion()));
        }
    }

    static void validateReadConcern(final AsyncConnection connection, final ReadConcern readConcern,
                                    final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throwable = new IllegalArgumentException(format("ReadConcern not supported by wire version: %s",
                    connection.getDescription().getMaxWireVersion()));
        }
        callable.call(connection, throwable);
    }

    static void validateReadConcern(final AsyncConnectionSource source, final AsyncConnection connection, final ReadConcern readConcern,
                                    final AsyncCallableWithConnectionAndSource callable) {
        validateReadConcern(connection, readConcern, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void validateCollation(final Connection connection, final Collation collation) {
        validateCollation(connection.getDescription(), collation);
    }

    static void validateCollation(final ConnectionDescription connectionDescription, final Collation collation) {
        if (collation != null && !serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        }
    }

    static void validateCollationAndWriteConcern(final ConnectionDescription connectionDescription, final Collation collation,
                                                 final WriteConcern writeConcern) {
        if (collation != null && !serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (collation != null && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying collation with an unacknowledged WriteConcern is not supported");
        }
    }

    private static void validateArrayFilters(final ConnectionDescription connectionDescription, final WriteConcern writeConcern) {
        if (serverIsLessThanVersionThreeDotSix(connectionDescription)) {
            throw new IllegalArgumentException(format("Array filters not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying array filters with an unacknowledged WriteConcern is not supported");
        }
    }

    private static void validateWriteRequestHint(final ConnectionDescription connectionDescription, final WriteConcern writeConcern,
                                                 final WriteRequest request) {
        if (serverIsLessThanVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if ((request instanceof DeleteRequest || request instanceof UpdateRequest) && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying hints with an unacknowledged WriteConcern is not supported");
        }
    }

    static void validateHint(final ConnectionDescription connectionDescription, final WriteConcern writeConcern) {
        if (serverIsLessThanVersionFourDotTwo(connectionDescription)) {
            throw new IllegalArgumentException(format("Hint not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying hints with an unacknowledged WriteConcern is not supported");
        }
    }

    static void validateAllowDiskUse(final Connection connection, final Boolean allowDiskUse) {
        validateAllowDiskUse(connection.getDescription(), allowDiskUse).ifPresent(throwable -> {
            throw new IllegalArgumentException(throwable.getMessage());
        });
    }

    static void validateAllowDiskUse(final AsyncConnection connection, final Boolean allowDiskUse,
                                     final AsyncCallableWithConnection callable) {
        Optional<Throwable> throwable = validateAllowDiskUse(connection.getDescription(), allowDiskUse);
        callable.call(connection, throwable.isPresent() ? throwable.get() : null);
    }

    static void validateCollation(final AsyncConnection connection, final Collation collation,
                                  final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throwable = new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connection.getDescription().getMaxWireVersion()));
        }
        callable.call(connection, throwable);
    }

    static void validateCollation(final AsyncConnectionSource source, final AsyncConnection connection,
                                  final Collation collation, final AsyncCallableWithConnectionAndSource callable) {
        validateCollation(connection, collation, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void validateWriteRequestCollations(final ConnectionDescription connectionDescription,
                                               final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        Collation collation = null;
        for (WriteRequest request : requests) {
            if (request instanceof UpdateRequest) {
                collation = ((UpdateRequest) request).getCollation();
            } else if (request instanceof DeleteRequest) {
                collation = ((DeleteRequest) request).getCollation();
            }
            if (collation != null) {
                break;
            }
        }
        validateCollationAndWriteConcern(connectionDescription, collation, writeConcern);
    }

    static void validateUpdateRequestArrayFilters(final ConnectionDescription connectionDescription,
                                                  final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        for (WriteRequest request : requests) {
            List<BsonDocument> arrayFilters = null;
            if (request instanceof UpdateRequest) {
                arrayFilters = ((UpdateRequest) request).getArrayFilters();
            }
            if (arrayFilters != null) {
                validateArrayFilters(connectionDescription, writeConcern);
                break;
            }
        }
    }

    static void validateWriteRequestHints(final ConnectionDescription connectionDescription,
                                          final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        for (WriteRequest request : requests) {
            Bson hint = null;
            String hintString = null;
            if (request instanceof UpdateRequest) {
                hint = ((UpdateRequest) request).getHint();
                hintString = ((UpdateRequest) request).getHintString();
            } else if (request instanceof DeleteRequest) {
                hint = ((DeleteRequest) request).getHint();
                hintString = ((DeleteRequest) request).getHintString();
            }
            if (hint != null || hintString != null) {
                validateWriteRequestHint(connectionDescription, writeConcern, request);
                break;
            }
        }
    }

    static void validateWriteRequests(final ConnectionDescription connectionDescription, final Boolean bypassDocumentValidation,
                                      final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        checkBypassDocumentValidationIsSupported(connectionDescription, bypassDocumentValidation, writeConcern);
        validateWriteRequestCollations(connectionDescription, requests, writeConcern);
        validateUpdateRequestArrayFilters(connectionDescription, requests, writeConcern);
        validateWriteRequestHints(connectionDescription, requests, writeConcern);
    }

    static <R> boolean validateWriteRequestsAndCompleteIfInvalid(final ConnectionDescription connectionDescription,
            final Boolean bypassDocumentValidation, final List<? extends WriteRequest> requests, final WriteConcern writeConcern,
            final SingleResultCallback<R> callback) {
        try {
            validateWriteRequests(connectionDescription, bypassDocumentValidation, requests, writeConcern);
            return false;
        } catch (Throwable validationT) {
            callback.onResult(null, validationT);
            return true;
        }
    }

    static void validateIndexRequestCollations(final Connection connection, final List<IndexRequest> requests) {
        for (IndexRequest request : requests) {
            if (request.getCollation() != null) {
                validateCollation(connection, request.getCollation());
                break;
            }
        }
    }

    static void validateIndexRequestCollations(final AsyncConnection connection, final List<IndexRequest> requests,
                                               final AsyncCallableWithConnection callable) {
        boolean calledTheCallable = false;
        for (IndexRequest request : requests) {
            if (request.getCollation() != null) {
                calledTheCallable = true;
                validateCollation(connection, request.getCollation(), new AsyncCallableWithConnection() {
                    @Override
                    public void call(final AsyncConnection connection, final Throwable t) {
                        callable.call(connection, t);
                    }
                });
                break;
            }
        }
        if (!calledTheCallable) {
            callable.call(connection, null);
        }
    }

    static void validateFindOptions(final Connection connection, final ReadConcern readConcern, final Collation collation,
                                    final Boolean allowDiskUse) {
        validateReadConcernAndCollation(connection, readConcern, collation);
        validateAllowDiskUse(connection, allowDiskUse);
    }

    static void validateFindOptions(final ConnectionDescription description, final ReadConcern readConcern,
                                    final Collation collation, final Boolean allowDiskUse) {
        validateReadConcernAndCollation(description, readConcern, collation);
        validateAllowDiskUse(description, allowDiskUse).ifPresent(throwable -> {
            throw new IllegalArgumentException(throwable.getMessage());
        });
    }

    static void validateReadConcernAndCollation(final Connection connection, final ReadConcern readConcern,
                                                final Collation collation) {
        validateReadConcern(connection, readConcern);
        validateCollation(connection, collation);
    }

    static void validateReadConcernAndCollation(final ConnectionDescription description, final ReadConcern readConcern,
                                                final Collation collation) {
        validateReadConcern(description, readConcern);
        validateCollation(description, collation);
    }

    static void validateFindOptions(final AsyncConnection connection, final ReadConcern readConcern,
                                    final Collation collation, final Boolean allowDiskUse,
                                    final AsyncCallableWithConnection callable) {
        validateReadConcernAndCollation(connection, readConcern, collation, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callable.call(connection, t);
                } else {
                    validateAllowDiskUse(connection, allowDiskUse, callable);
                }
            }
        });
    }

    static void validateFindOptions(final AsyncConnectionSource source, final AsyncConnection connection, final ReadConcern readConcern,
                                    final Collation collation, final Boolean allowDiskUse,
                                    final AsyncCallableWithConnectionAndSource callable) {
        validateFindOptions(connection, readConcern, collation, allowDiskUse, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void validateReadConcernAndCollation(final AsyncConnection connection, final ReadConcern readConcern,
                                                final Collation collation, final AsyncCallableWithConnection callable) {
        validateReadConcern(connection, readConcern, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callable.call(connection, t);
                } else {
                    validateCollation(connection, collation, callable);
                }
            }
        });
    }

    static void validateReadConcernAndCollation(final AsyncConnectionSource source, final AsyncConnection connection,
                                                final ReadConcern readConcern, final Collation collation,
                                                final AsyncCallableWithConnectionAndSource callable) {
        validateReadConcernAndCollation(connection, readConcern, collation, new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                callable.call(source, connection, t);
            }
        });
    }

    static void checkBypassDocumentValidationIsSupported(final ConnectionDescription connectionDescription,
                                                         final Boolean bypassDocumentValidation, final WriteConcern writeConcern) {
        if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(connectionDescription)
                && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying bypassDocumentValidation with an unacknowledged WriteConcern is not supported");
        }
    }

    static boolean isRetryableWrite(final boolean retryWrites, final WriteConcern writeConcern,
                                    final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                    final SessionContext sessionContext) {
        if (!retryWrites) {
            return false;
        } else if (!writeConcern.isAcknowledged()) {
            LOGGER.debug("retryWrites set to true but the writeConcern is unacknowledged.");
            return false;
        } else if (sessionContext.hasActiveTransaction()) {
            LOGGER.debug("retryWrites set to true but in an active transaction.");
            return false;
        } else {
            return canRetryWrite(serverDescription, connectionDescription, sessionContext);
        }
    }

    static boolean canRetryWrite(final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                 final SessionContext sessionContext) {
        if (serverIsLessThanVersionThreeDotSix(connectionDescription)) {
            LOGGER.debug("retryWrites set to true but the server does not support retryable writes.");
            return false;
        } else if (serverDescription.getLogicalSessionTimeoutMinutes() == null && serverDescription.getType() != ServerType.LOAD_BALANCER) {
            LOGGER.debug("retryWrites set to true but the server does not have 3.6 feature compatibility enabled.");
            return false;
        } else if (connectionDescription.getServerType().equals(ServerType.STANDALONE)) {
            LOGGER.debug("retryWrites set to true but the server is a standalone server.");
            return false;
        } else if (!sessionContext.hasSession()) {
            LOGGER.debug("retryWrites set to true but there is no implicit session, likely because the MongoClient was created with "
                    + "multiple MongoCredential instances and sessions can only be used with a single MongoCredential");
            return false;
        }
        return true;
    }

    static boolean isRetryableRead(final boolean retryReads, final ServerDescription serverDescription,
                                   final ConnectionDescription connectionDescription, final SessionContext sessionContext) {
        if (!retryReads) {
            return false;
        } else if (sessionContext.hasActiveTransaction()) {
            LOGGER.debug("retryReads set to true but in an active transaction.");
            return false;
        } else {
            return canRetryRead(serverDescription, connectionDescription, sessionContext);
        }
    }

    static boolean canRetryRead(final ServerDescription serverDescription, final ConnectionDescription connectionDescription,
                                final SessionContext sessionContext) {
        if (serverIsLessThanVersionThreeDotSix(connectionDescription)) {
            LOGGER.debug("retryReads set to true but the server does not support retryable reads.");
            return false;
        } else if (serverDescription.getLogicalSessionTimeoutMinutes() == null && serverDescription.getType() != ServerType.LOAD_BALANCER) {
            LOGGER.debug("retryReads set to true but the server does not have 3.6 feature compatibility enabled.");
            return false;
        } else if (serverDescription.getType() != ServerType.STANDALONE && !sessionContext.hasSession()) {
            LOGGER.debug("retryReads set to true but there is no implicit session, likely because the MongoClient was created with "
                    + "multiple MongoCredential instances and sessions can only be used with a single MongoCredential");
            return false;
        }
        return true;
    }

    static <T> QueryBatchCursor<T> createEmptyBatchCursor(final MongoNamespace namespace, final Decoder<T> decoder,
                                                          final ServerAddress serverAddress, final int batchSize) {
        return new QueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L,
                serverAddress),
                0, batchSize, decoder);
    }

    static <T> AsyncBatchCursor<T> createEmptyAsyncBatchCursor(final MongoNamespace namespace, final ServerAddress serverAddress) {
        return new AsyncSingleBatchQueryCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L, serverAddress));
    }

    static <T> BatchCursor<T> cursorDocumentToBatchCursor(final BsonDocument cursorDocument, final Decoder<T> decoder,
                                                          final ConnectionSource source, final Connection connection, final int batchSize) {
        return new QueryBatchCursor<T>(OperationHelper.<T>cursorDocumentToQueryResult(cursorDocument,
                source.getServerDescription().getAddress()),
                0, batchSize, 0, decoder, source, connection);
    }

    static <T> AsyncBatchCursor<T> cursorDocumentToAsyncBatchCursor(final BsonDocument cursorDocument, final Decoder<T> decoder,
                                                                    final AsyncConnectionSource source, final AsyncConnection connection,
                                                                    final int batchSize) {
        return new AsyncQueryBatchCursor<T>(OperationHelper.<T>cursorDocumentToQueryResult(cursorDocument,
                source.getServerDescription().getAddress()),
                0, batchSize, 0, decoder, source, connection, cursorDocument);
    }


    static <T> QueryResult<T> cursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress, "firstBatch");
    }

    static <T> QueryResult<T> getMoreCursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return cursorDocumentToQueryResult(cursorDocument, serverAddress, "nextBatch");
    }

    private static <T> QueryResult<T> cursorDocumentToQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress,
                                                                  final String fieldNameContainingBatch) {
        long cursorId = ((BsonInt64) cursorDocument.get("id")).getValue();
        MongoNamespace queryResultNamespace = new MongoNamespace(cursorDocument.getString("ns").getValue());
        return new QueryResult<T>(queryResultNamespace, BsonDocumentWrapperHelper.<T>toList(cursorDocument, fieldNameContainingBatch),
                cursorId, serverAddress);
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, singletonList(connection));
    }

    private static class ReferenceCountedReleasingWrappedCallback<T> implements SingleResultCallback<T> {
        private final SingleResultCallback<T> wrapped;
        private final List<? extends ReferenceCounted> referenceCounted;

        ReferenceCountedReleasingWrappedCallback(final SingleResultCallback<T> wrapped,
                                                 final List<? extends ReferenceCounted> referenceCounted) {
            this.wrapped = wrapped;
            this.referenceCounted = notNull("referenceCounted", referenceCounted);
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            for (ReferenceCounted cur : referenceCounted) {
                if (cur != null) {
                    cur.release();
                }
            }
            wrapped.onResult(result, t);
        }
    }

    static <T> T withReadConnectionSource(final ReadBinding binding, final CallableWithSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return callable.call(source);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final WriteBinding binding, final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnection<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(connection);
        } finally {
            connection.release();
        }
    }

    /**
     * Gets a {@link ConnectionSource} and a {@link Connection} from the {@code sourceSupplier} and executes the {@code function} with them.
     * Guarantees to {@linkplain ReferenceCounted#release() release} the source and the connection after completion of the {@code function}.
     *
     * @param wrapSourceConnectionException See {@link #withSuppliedResource(Supplier, boolean, Function)}.
     * @see #withAsyncSourceAndConnection(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackBiFunction)
     */
    static <R> R withSourceAndConnection(final Supplier<ConnectionSource> sourceSupplier,
            final boolean wrapSourceConnectionException,
            final BiFunction<ConnectionSource, Connection, R> function) throws SourceOrConnectionInternalException {
        return withSuppliedResource(sourceSupplier, wrapSourceConnectionException, source ->
                withSuppliedResource(source::getConnection, wrapSourceConnectionException, connection ->
                        function.apply(source, connection)));
    }

    /**
     * Gets a {@link ReferenceCounted} resource from the {@code resourceSupplier} and applies the {@code function} to it.
     * Guarantees to {@linkplain ReferenceCounted#release() release} the resource after completion of the {@code function}.
     *
     * @param wrapSupplierException If {@code true} and {@code resourceSupplier} completes abruptly, then the exception is wrapped
     * into {@link SourceOrConnectionInternalException}, such that it can be accessed
     * via {@link SourceOrConnectionInternalException#getCause()}.
     * @see #withAsyncSuppliedResource(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackFunction)
     */
    static <R, T extends ReferenceCounted> R withSuppliedResource(final Supplier<T> resourceSupplier,
            final boolean wrapSupplierException, final Function<T, R> function) throws SourceOrConnectionInternalException {
        T resource = null;
        try {
            try {
                resource = resourceSupplier.get();
            } catch (RuntimeException supplierException) {
                if (wrapSupplierException) {
                    throw new SourceOrConnectionInternalException(supplierException);
                } else {
                    throw supplierException;
                }
            }
            return function.apply(resource);
        } finally {
            if (resource != null) {
                resource.release();
            }
        }
    }

    static void withAsyncConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnection callable) {
        binding.getWriteConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable), LOGGER));
    }

    static void withAsyncConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnectionAndSource callable) {
        binding.getWriteConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(callable), LOGGER));
    }

    static void withAsyncReadConnection(final AsyncReadBinding binding, final AsyncCallableWithSource callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithSourceCallback(callable), LOGGER));
    }

    /**
     * @see #withSourceAndConnection(Supplier, boolean, BiFunction)
     */
    static <R> void withAsyncSourceAndConnection(final AsyncCallbackSupplier<AsyncConnectionSource> sourceAsyncSupplier,
            final boolean wrapSourceConnectionException,
            final SingleResultCallback<R> callback,
            final AsyncCallbackBiFunction<AsyncConnectionSource, AsyncConnection, R> asyncFunction) throws SourceOrConnectionInternalException {
        SingleResultCallback<R> errorHandlingCallback = errorHandlingCallback(callback, LOGGER);
        withAsyncSuppliedResource(sourceAsyncSupplier, wrapSourceConnectionException, errorHandlingCallback,
                (source, sourceReleasingCallback) ->
                        withAsyncSuppliedResource(source::getConnection, wrapSourceConnectionException, sourceReleasingCallback,
                                (connection, connectionAndSourceReleasingCallback) ->
                                        asyncFunction.apply(source, connection, connectionAndSourceReleasingCallback)));
    }

    /**
     * @see #withSuppliedResource(Supplier, boolean, Function)
     */
    static <R, T extends ReferenceCounted> void withAsyncSuppliedResource(final AsyncCallbackSupplier<T> resourceSupplier,
            final boolean wrapSourceConnectionException, final SingleResultCallback<R> callback,
            final AsyncCallbackFunction<T, R> function) {
        SingleResultCallback<R> errorHandlingCallback = errorHandlingCallback(callback, LOGGER);
        resourceSupplier.get((resource, supplierException) -> {
            if (supplierException != null) {
                if (wrapSourceConnectionException) {
                    supplierException = new SourceOrConnectionInternalException(supplierException);
                }
                errorHandlingCallback.onResult(null, supplierException);
            } else {
                AsyncCallbackSupplier<R> curriedFunction = clbk -> function.apply(resource, clbk);
                curriedFunction.andFinally(resource::release)
                        .get(errorHandlingCallback);
            }
        });
    }

    private static class AsyncCallableWithConnectionCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnection callable;
        AsyncCallableWithConnectionCallback(final AsyncCallableWithConnection callable) {
            this.callable = callable;
        }
        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, t);
            } else {
                withAsyncConnectionSourceCallableConnection(source, callable);
            }
        }
    }

    private static class AsyncCallableWithSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithSource callable;
        AsyncCallableWithSourceCallback(final AsyncCallableWithSource callable) {
            this.callable = callable;
        }
        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, t);
            } else {
                withAsyncConnectionSource(source, callable);
            }
        }
    }

    private static void withAsyncConnectionSourceCallableConnection(final AsyncConnectionSource source,
                                                                    final AsyncCallableWithConnection callable) {
        source.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection connection, final Throwable t) {
                source.release();
                if (t != null) {
                    callable.call(null, t);
                } else {
                    callable.call(connection, null);
                }
            }
        });
    }

    private static void withAsyncConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithSource callable) {
        callable.call(source, null);
    }

    private static void withAsyncConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource callable) {
        source.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection result, final Throwable t) {
                callable.call(source, result, t);
            }
        });
    }

    private static class AsyncCallableWithConnectionAndSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnectionAndSource callable;

        AsyncCallableWithConnectionAndSourceCallback(final AsyncCallableWithConnectionAndSource callable) {
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, null, t);
            } else {
                withAsyncConnectionSource(source, callable);
            }
        }
    }

    private static Optional<Throwable> validateAllowDiskUse(final ConnectionDescription description, final Boolean allowDiskUse) {
        Optional<Throwable> throwable = Optional.empty();
        if (allowDiskUse != null && serverIsLessThanVersionThreeDotTwo(description)) {
            throwable = Optional.of(new IllegalArgumentException(format("allowDiskUse not supported by wire version: %s",
                    description.getMaxWireVersion())));
        }
        return throwable;
    }

    private OperationHelper() {
    }

    /**
     * This internal exception is used to
     * <ul>
     *     <li>on one hand allow propagating exceptions from {@link #withSourceAndConnection(Supplier, boolean, BiFunction)} /
     *     {@link #withAsyncSourceAndConnection(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackBiFunction)}
     *     so that they can be properly retried, which is useful, e.g., for {@link com.mongodb.MongoConnectionPoolClearedException};</li>
     *     <li>on the other hand to prevent them from propagation once the retry decision is made.</li>
     * </ul>
     *
     * @see #withSourceAndConnection(Supplier, boolean, BiFunction)
     * @see #withAsyncSourceAndConnection(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackBiFunction)
     */
    static final class SourceOrConnectionInternalException extends RuntimeException {
        private static final long serialVersionUID = 0;

        private SourceOrConnectionInternalException(final Throwable cause) {
            super(assertNotNull(cause));
        }

        @NonNull
        @Override
        public Throwable getCause() {
            return assertNotNull(super.getCause());
        }
    }
}

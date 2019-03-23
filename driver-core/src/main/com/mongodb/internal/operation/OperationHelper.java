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
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.IndexRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.Decoder;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionThreeDotSix;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

final class OperationHelper {
    public static final Logger LOGGER = Loggers.getLogger("operation");

    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithSource<T> {
        T call(ConnectionSource source);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ConnectionSource source, Connection connection);
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
        if (!ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo(description) && !readConcern.isServerDefault()) {
            throw new IllegalArgumentException(format("ReadConcern not supported by wire version: %s",
                    description.getMaxWireVersion()));
        }
    }

    static void validateReadConcern(final AsyncConnection connection, final ReadConcern readConcern,
                                    final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
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
        if (collation != null && !ServerVersionHelper.serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        }
    }

    static void validateCollationAndWriteConcern(final ConnectionDescription connectionDescription, final Collation collation,
                                                 final WriteConcern writeConcern) {
        if (collation != null && !ServerVersionHelper.serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
            throw new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connectionDescription.getMaxWireVersion()));
        } else if (collation != null && !writeConcern.isAcknowledged()) {
            throw new MongoClientException("Specifying collation with an unacknowledged WriteConcern is not supported");
        }
    }

    static void validateCollation(final AsyncConnection connection, final Collation collation,
                                  final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!ServerVersionHelper.serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
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

    static void validateWriteRequests(final ConnectionDescription connectionDescription, final Boolean bypassDocumentValidation,
                                      final List<? extends WriteRequest> requests, final WriteConcern writeConcern) {
        checkBypassDocumentValidationIsSupported(connectionDescription, bypassDocumentValidation, writeConcern);
        validateWriteRequestCollations(connectionDescription, requests, writeConcern);
    }

    static void validateWriteRequests(final AsyncConnection connection, final Boolean bypassDocumentValidation,
                                      final List<? extends WriteRequest> requests, final WriteConcern writeConcern,
                                      final AsyncCallableWithConnection callable) {
        try {
            validateWriteRequests(connection.getDescription(), bypassDocumentValidation, requests, writeConcern);
            callable.call(connection, null);
        } catch (Throwable t) {
            callable.call(connection, t);
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

    static void validateReadConcernAndCollation(final AsyncConnection connection, final ReadConcern readConcern,
                                                final Collation collation,
                                                final AsyncCallableWithConnection callable) {
        validateReadConcern(connection, readConcern, new AsyncCallableWithConnection(){
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
        if (bypassDocumentValidation != null && ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo(connectionDescription)
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
        } else if (serverDescription.getLogicalSessionTimeoutMinutes() == null) {
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
        } else if (serverDescription.getLogicalSessionTimeoutMinutes() == null) {
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
                                                          final ConnectionSource source, final int batchSize) {
        return new QueryBatchCursor<T>(OperationHelper.<T>cursorDocumentToQueryResult(cursorDocument,
                source.getServerDescription().getAddress()),
                0, batchSize, decoder, source);
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

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, singletonList(source));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, singletonList(connection));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source,
                                                         final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, asList(connection, source));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped,
                                                         final AsyncReadBinding readBinding,
                                                         final AsyncConnectionSource source,
                                                         final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<T>(wrapped, asList(readBinding, connection, source));
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

    static class ConnectionReleasingWrappedCallback<T> implements SingleResultCallback<T> {
        private final SingleResultCallback<T> wrapped;
        private final AsyncConnectionSource source;
        private final AsyncConnection connection;

        ConnectionReleasingWrappedCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source,
                                           final AsyncConnection connection) {
            this.wrapped = wrapped;
            this.source = notNull("source", source);
            this.connection = notNull("connection", connection);
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            connection.release();
            source.release();
            wrapped.onResult(result, t);
        }

        public SingleResultCallback<T> releaseConnectionAndGetWrapped() {
            connection.release();
            source.release();
            return wrapped;
        }
    }

    static <T> T withConnection(final ReadBinding binding, final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final ReadBinding binding, final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
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

    static <T> T withReleasableConnection(final ReadBinding binding, final MongoException connectionException,
                                          final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = null;
        Connection connection;
        try {
            source = binding.getReadConnectionSource();
            connection = source.getConnection();
        } catch (Throwable t){
            if (source != null) {
                source.release();
            }
            throw connectionException;
        }
        try {
            return callable.call(source, connection);
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

    static <T> T withReleasableConnection(final WriteBinding binding, final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return callable.call(source, source.getConnection());
        } finally {
            source.release();
        }
    }

    static <T> T withReleasableConnection(final WriteBinding binding, final MongoException connectionException,
                                          final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = null;
        Connection connection;
        try {
            source = binding.getWriteConnectionSource();
            connection = source.getConnection();
        } catch (Throwable t){
            if (source != null) {
                source.release();
            }
            throw connectionException;
        }
        try {
            return callable.call(source, connection);
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

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnectionAndSource<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(source, connection);
        } finally {
            connection.release();
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

    static void withAsyncReadConnection(final AsyncReadBinding binding, final AsyncCallableWithConnectionAndSource callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(callable), LOGGER));
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

    private OperationHelper() {
    }
}

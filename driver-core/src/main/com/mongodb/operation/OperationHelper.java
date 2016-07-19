/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.ReferenceCounted;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerVersion;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.Decoder;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

final class OperationHelper {
    public static final Logger LOGGER = Loggers.getLogger("operation");

    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ConnectionSource source, Connection connection);
    }

    interface AsyncCallableWithConnection {
        void call(AsyncConnection connection, Throwable t);
    }

    interface AsyncCallableWithConnectionAndSource {
        void call(AsyncConnectionSource source, AsyncConnection connection, Throwable t);
    }

    static void checkValidReadConcern(final Connection connection, final ReadConcern readConcern) {
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throw new IllegalArgumentException(format("Unsupported ReadConcern : '%s'", readConcern.asDocument().toJson()));
        }
    }

    static void checkValidReadConcern(final AsyncConnection connection, final ReadConcern readConcern,
                                      final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throwable = new IllegalArgumentException(format("Unsupported ReadConcern : '%s'", readConcern.asDocument().toJson()));
        }
        callable.call(connection, throwable);
    }

    static void checkValidReadConcern(final AsyncConnectionSource source, final AsyncConnection connection, final ReadConcern readConcern,
                                      final AsyncCallableWithConnectionAndSource callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throwable = new IllegalArgumentException(format("Unsupported ReadConcern : '%s'", readConcern.asDocument().toJson()));
        }
        callable.call(source, connection, throwable);
    }

    static boolean bypassDocumentValidationNotSupported(final Boolean bypassDocumentValidation, final WriteConcern writeConcern,
                                                        final ConnectionDescription description) {
        return bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(description) && !writeConcern.isAcknowledged();
    }

    static MongoClientException getBypassDocumentValidationException() {
        return new MongoClientException("Specifying bypassDocumentValidation with an unacknowledged WriteConcern "
                                        + "is not supported");
    }

    static <T> QueryBatchCursor<T> createEmptyBatchCursor(final MongoNamespace namespace, final Decoder<T> decoder,
                                                          final ServerAddress serverAddress, final int batchSize) {
        return new QueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L,
                                                          serverAddress),
                                       0, batchSize, decoder);
    }

    static <T> AsyncBatchCursor<T> createEmptyAsyncBatchCursor(final MongoNamespace namespace, final Decoder<T> decoder,
                                                               final ServerAddress serverAddress, final int batchSize) {
        return new AsyncQueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L, serverAddress), 0, batchSize,
                decoder);
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
                                            0, batchSize, 0, decoder, source, connection);
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
                cur.release();
            }
            wrapped.onResult(result, t);
        }
    }

    static boolean serverIsAtLeastVersionTwoDotSix(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(2, 6));
    }

    static boolean serverIsAtLeastVersionThreeDotZero(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(asList(3, 0, 0)));
    }

    static boolean serverIsAtLeastVersionThreeDotTwo(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(asList(3, 1, 9)));
    }

    static boolean serverIsAtLeastVersionThreeDotFour(final ConnectionDescription description) {
        return serverIsAtLeastVersion(description, new ServerVersion(asList(3, 3, 8)));
    }

    static boolean serverIsAtLeastVersion(final ConnectionDescription description, final ServerVersion serverVersion) {
        return description.getServerVersion().compareTo(serverVersion) >= 0;
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

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnectionAndSource<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(source, connection);
        } finally {
            connection.release();
        }
    }

    static void withConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnection callable) {
        binding.getWriteConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable), LOGGER));
    }

    static void withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnection callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable), LOGGER));
    }

    static void withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnectionAndSource callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(callable), LOGGER));
    }

    private static class AsyncCallableWithConnectionCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnection callable;
        public AsyncCallableWithConnectionCallback(final AsyncCallableWithConnection callable) {
            this.callable = callable;
        }
        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, t);
            } else {
                withConnectionSource(source, callable);
            }
        }
    }

    private static void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnection callable) {
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

    private static void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource callable) {
        source.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection result, final Throwable t) {
                callable.call(source, result, t);
            }
        });
    }

    private static class AsyncCallableWithConnectionAndSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnectionAndSource callable;

        public AsyncCallableWithConnectionAndSourceCallback(final AsyncCallableWithConnectionAndSource callable) {
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(null, null, t);
            } else {
                withConnectionSource(source, callable);
            }
        }
    }

    private OperationHelper() {
    }
}

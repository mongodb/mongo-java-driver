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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.QueryResult;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

final class AsyncOperationHelper {

    interface AsyncCallableWithConnection {
        void call(ClientSideOperationTimeout clientSideOperationTimeout, AsyncConnection connection, Throwable t);
    }

    interface AsyncCallableWithSource {
        void call(ClientSideOperationTimeout clientSideOperationTimeout, AsyncConnectionSource source, Throwable t);
    }

    interface AsyncCallableWithConnectionAndSource {
        void call(ClientSideOperationTimeout clientSideOperationTimeout, AsyncConnectionSource source, AsyncConnection connection,
                  Throwable t);
    }

    static void validateReadConcern(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                                    final ReadConcern readConcern, final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotTwo(connection.getDescription()) && !readConcern.isServerDefault()) {
            throwable = new IllegalArgumentException(format("ReadConcern not supported by wire version: %s",
                    connection.getDescription().getMaxWireVersion()));
        }
        callable.call(clientSideOperationTimeout, connection, throwable);
    }


    static void validateAllowDiskUse(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                                     final Boolean allowDiskUse, final AsyncCallableWithConnection callable) {
        Optional<Throwable> throwable = OperationHelper.validateAllowDiskUse(connection.getDescription(), allowDiskUse);
        callable.call(clientSideOperationTimeout, connection, throwable.orElse(null));
    }

    static void validateCollation(final ClientSideOperationTimeout clientSideOperationTimeout,
                                  final AsyncConnectionSource source,
                                  final AsyncConnection connection,
                                  final Collation collation,
                                  final AsyncCallableWithConnectionAndSource callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throwable = new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connection.getDescription().getMaxWireVersion()));
        }
        callable.call(clientSideOperationTimeout, source, connection, throwable);
    }

    static void validateCollation(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                                  final Collation collation, final AsyncCallableWithConnection callable) {
        Throwable throwable = null;
        if (!serverIsAtLeastVersionThreeDotFour(connection.getDescription()) && collation != null) {
            throwable = new IllegalArgumentException(format("Collation not supported by wire version: %s",
                    connection.getDescription().getMaxWireVersion()));
        }
        callable.call(clientSideOperationTimeout, connection, throwable);
    }

    static void validateWriteRequests(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                                      final Boolean bypassDocumentValidation, final List<? extends WriteRequest> requests,
                                      final WriteConcern writeConcern, final AsyncCallableWithConnection callable) {
        try {
            OperationHelper.validateWriteRequests(connection.getDescription(), bypassDocumentValidation, requests, writeConcern);
            callable.call(clientSideOperationTimeout, connection, null);
        } catch (Throwable t) {
            callable.call(clientSideOperationTimeout, connection, t);
        }
    }

    static void validateIndexRequestCollations(final ClientSideOperationTimeout clientSideOperationTimeout,
                                               final AsyncConnection connection, final List<IndexRequest> requests,
                                               final AsyncCallableWithConnection callable) {
        boolean calledTheCallable = false;
        for (IndexRequest request : requests) {
            if (request.getCollation() != null) {
                calledTheCallable = true;
                validateCollation(clientSideOperationTimeout, connection, request.getCollation(), callable);
                break;
            }
        }
        if (!calledTheCallable) {
            callable.call(clientSideOperationTimeout, connection, null);
        }
    }

    static void validateFindOptions(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                                    final ReadConcern readConcern, final Collation collation, final Boolean allowDiskUse,
                                    final AsyncCallableWithConnection callable) {
        validateReadConcernAndCollation(clientSideOperationTimeout, connection, readConcern, collation,
                (clientSideOperationTimeout1, connection1, t) -> {
                    if (t != null) {
                        callable.call(clientSideOperationTimeout1, connection1, t);
                    } else {
                        validateAllowDiskUse(clientSideOperationTimeout1, connection1, allowDiskUse, callable);
                    }
                });
    }

    static void validateFindOptions(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                                    final AsyncConnection connection, final ReadConcern readConcern,
                                    final Collation collation, final Boolean allowDiskUse,
                                    final AsyncCallableWithConnectionAndSource callable) {
        validateFindOptions(clientSideOperationTimeout, connection, readConcern, collation, allowDiskUse,
                (clientSideOperationTimeout1, connection1, t) -> callable.call(clientSideOperationTimeout1, source, connection1, t));
    }

    static void validateReadConcernAndCollation(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                final AsyncConnection connection, final ReadConcern readConcern,
                                                final Collation collation, final AsyncCallableWithConnection callable) {
        validateReadConcern(clientSideOperationTimeout, connection, readConcern, (clientSideOperationTimeout1, connection1, t) -> {
            if (t != null) {
                callable.call(clientSideOperationTimeout1, connection1, t);
            } else {
                validateCollation(clientSideOperationTimeout1, connection1, collation, callable);
            }
        });
    }


    static <T> AsyncBatchCursor<T> createEmptyAsyncBatchCursor(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                               final MongoNamespace namespace, final ServerAddress serverAddress) {
        return new AsyncSingleBatchQueryCursor<>(clientSideOperationTimeout, new QueryResult<>(namespace, Collections.emptyList(), 0L,
                serverAddress));
    }

    static <T> AsyncBatchCursor<T> cursorDocumentToAsyncBatchCursor(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                                    final BsonDocument cursorDocument, final Decoder<T> decoder,
                                                                    final AsyncConnectionSource source, final AsyncConnection connection,
                                                                    final int batchSize) {
        return new AsyncQueryBatchCursor<>(clientSideOperationTimeout, OperationHelper.cursorDocumentToQueryResult(cursorDocument,
                source.getServerDescription().getAddress()), 0, batchSize, decoder, source, connection, cursorDocument);
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source) {
        return new ReferenceCountedReleasingWrappedCallback<>(wrapped, singletonList(source));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<>(wrapped, singletonList(connection));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source,
                                                         final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<>(wrapped, asList(connection, source));
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped,
                                                         final AsyncReadBinding readBinding,
                                                         final AsyncConnectionSource source,
                                                         final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<>(wrapped, asList(readBinding, connection, source));
    }

    static void withAsyncConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncWriteBinding binding,
                                    final AsyncCallableWithConnection callable) {
        binding.getWriteConnectionSource(
                errorHandlingCallback(new AsyncCallableWithConnectionCallback(clientSideOperationTimeout, callable), LOGGER));
    }

    static void withAsyncConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncWriteBinding binding,
                                    final AsyncCallableWithConnectionAndSource callable) {
        binding.getWriteConnectionSource(
                errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(clientSideOperationTimeout, callable), LOGGER));
    }

    static void withAsyncReadConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncReadBinding binding,
                                        final AsyncCallableWithSource callable) {
        binding.getReadConnectionSource(
                errorHandlingCallback(new AsyncCallableWithSourceCallback(clientSideOperationTimeout, callable), LOGGER));
    }

    static void withAsyncReadConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncReadBinding binding,
                                        final AsyncCallableWithConnectionAndSource callable) {
        binding.getReadConnectionSource(
                errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(clientSideOperationTimeout, callable), LOGGER));
    }

    private static void withAsyncConnectionSourceCallableConnection(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                                    final AsyncConnectionSource source,
                                                                    final AsyncCallableWithConnection callable) {
        source.getConnection((connection, t) -> {
            source.release();
            if (t != null) {
                callable.call(clientSideOperationTimeout, null, t);
            } else {
                callable.call(clientSideOperationTimeout, connection, null);
            }
        });
    }

    private static void withAsyncConnectionSource(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                  final AsyncConnectionSource source, final AsyncCallableWithSource callable) {
        callable.call(clientSideOperationTimeout, source, null);
    }

    private static void withAsyncConnectionSource(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                  final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource callable) {
        source.getConnection((result, t) -> callable.call(clientSideOperationTimeout, source, result, t));
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

    private static class AsyncCallableWithConnectionCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final ClientSideOperationTimeout clientSideOperationTimeout;
        private final AsyncCallableWithConnection callable;
        AsyncCallableWithConnectionCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                            final AsyncCallableWithConnection callable) {
            this.clientSideOperationTimeout = clientSideOperationTimeout;
            this.callable = callable;
        }
        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(clientSideOperationTimeout, null, t);
            } else {
                withAsyncConnectionSourceCallableConnection(clientSideOperationTimeout, source, callable);
            }
        }
    }

    private static class AsyncCallableWithSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final ClientSideOperationTimeout clientSideOperationTimeout;
        private final AsyncCallableWithSource callable;
        AsyncCallableWithSourceCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                        final AsyncCallableWithSource callable) {
            this.clientSideOperationTimeout = clientSideOperationTimeout;
            this.callable = callable;
        }
        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(clientSideOperationTimeout, null, t);
            } else {
                withAsyncConnectionSource(clientSideOperationTimeout, source, callable);
            }
        }
    }

    private static class AsyncCallableWithConnectionAndSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final ClientSideOperationTimeout clientSideOperationTimeout;
        private final AsyncCallableWithConnectionAndSource callable;

        AsyncCallableWithConnectionAndSourceCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final AsyncCallableWithConnectionAndSource callable) {
            this.clientSideOperationTimeout = clientSideOperationTimeout;
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callable.call(clientSideOperationTimeout, null, null, t);
            } else {
                withAsyncConnectionSource(clientSideOperationTimeout, source, callable);
            }
        }
    }

    private AsyncOperationHelper(){}

}

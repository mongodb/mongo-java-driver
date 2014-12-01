/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.Function;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerVersion;

import java.util.List;

import static com.mongodb.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.util.Arrays.asList;

final class OperationHelper {

    // TODO: This is duplicated in ProtocolHelper, but I don't want it to be public
    static final List<Integer> DUPLICATE_KEY_ERROR_CODES = asList(11000, 11001, 12582);


    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ConnectionSource source, Connection connection);
    }

    interface AsyncCallableWithConnection {
        void call(Connection connection, Throwable t);
    }

    interface AsyncCallableWithConnectionAndSource {
        void call(AsyncConnectionSource source, Connection connection, Throwable t);
    }

    static class IdentityTransformer<T> implements Function<T, T> {
        @Override
        public T apply(final T t) {
            return t;
        }
    }

    static class VoidTransformer<T> implements Function<T, Void> {
        @Override
        public Void apply(final T t) {
            return null;
        }
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final Connection connection) {
        return new ConnectionReleasingWrappedCallback<T>(wrapped, null, connection);
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source,
                                                         final Connection connection) {
        return new ConnectionReleasingWrappedCallback<T>(wrapped, source, connection);
    }

    private static class ConnectionReleasingWrappedCallback<T> implements SingleResultCallback<T> {
        private final SingleResultCallback<T> wrapped;
        private final AsyncConnectionSource source;
        private final Connection connection;

        ConnectionReleasingWrappedCallback(final SingleResultCallback<T> wrapped, final AsyncConnectionSource source,
                                           final Connection connection) {
            this.wrapped = wrapped;
            this.source = source;
            this.connection = connection;
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            if (connection != null) {
                connection.release();
            }
            if (source != null) {
                source.release();
            }
            wrapped.onResult(result, t);
        }
    }

    static boolean serverIsAtLeastVersionTwoDotSix(final Connection connection) {
        return serverIsAtLeastVersion(connection, new ServerVersion(2, 6));
    }

    static boolean serverIsAtLeastVersionTwoDotEight(final Connection connection) {
        // TODO: update to 2.8 once released
        return serverIsAtLeastVersion(connection, new ServerVersion(asList(2, 7, 7)));
    }

    static boolean serverIsAtLeastVersion(final Connection connection, final ServerVersion serverVersion) {
        return connection.getDescription().getServerVersion().compareTo(serverVersion) >= 0;
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
        binding.getWriteConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable)));
    }

    static void withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnection callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable)));
    }

    static void withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnectionAndSource callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionAndSourceCallback(callable)));
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

    private static <T> void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnection callable) {
        source.getConnection(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final Throwable t) {
                source.release();
                if (t != null) {
                    callable.call(null, t);
                } else {
                    callable.call(connection, null);
                }
            }
        });
    }

    private static <T> void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource callable) {
        source.getConnection(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection result, final Throwable t) {
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

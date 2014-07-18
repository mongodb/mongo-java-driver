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

package org.mongodb.operation;

import com.mongodb.MongoException;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SingleResultCallback;
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.protocol.Protocol;

import java.util.Arrays;
import java.util.List;

final class OperationHelper {

    // TODO: This is duplicated in ProtocolHelper, but I don't want it to be public
    static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);


    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ConnectionSource source, Connection connection);
    }

    interface AsyncCallableWithConnection<T> {
        MongoFuture<T> call(Connection connection);
    }

    interface AsyncCallableWithConnectionAndSource<T> {
        MongoFuture<T> call(AsyncConnectionSource source, Connection connection);
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

    static boolean serverIsAtLeastVersionTwoDotSix(final Connection connection) {
        return connection.getServerDescription().getVersion().compareTo(new ServerVersion(2, 6)) >= 0;
    }

    static <T> T executeProtocol(final Protocol<T> protocol, final ConnectionSource source) {
        Connection connection = source.getConnection();
        try {
            return protocol.execute(connection);
        } finally {
            connection.release();
        }
    }

    static <T, R> R executeProtocol(final Protocol<T> protocol, final Connection connection, final Function<T, R> transformer) {
        return transformer.apply(protocol.execute(connection));
    }

    static <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol, final AsyncWriteBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnection<T>() {
            @Override
            public MongoFuture<T> call(final Connection connection) {
                return protocol.executeAsync(connection);
            }
        });
    }

    static <T, R> MongoFuture<R> executeProtocolAsync(final Protocol<T> protocol, final Connection connection,
                                                      final Function<T, R> transformer) {
        final SingleResultFuture<R> future = new SingleResultFuture<R>();
        protocol.executeAsync(connection).register(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    try {
                        R transformedResult = transformer.apply(result);
                        future.init(transformedResult, null);
                    } catch (MongoException e1) {
                        future.init(null, e1);
                    }
                }
            }
        });
        return future;
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

    static <T> MongoFuture<T> withConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnection<T> callable) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getWriteConnectionSource().register(new AsyncCallableWithConnectionCallback<T>(future, callable));
        return future;
    }

    static <T> MongoFuture<T> withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnection<T> callable) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new AsyncCallableWithConnectionCallback<T>(future, callable));
        return future;
    }

    static <T> MongoFuture<T> withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnectionAndSource<T> callable) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new AsyncCallableWithConnectionAndSourceCallback<T>(future, callable));
        return future;
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

    static <T> void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnection<T> callable,
                                         final SingleResultFuture<T> future) {
        source.getConnection().register(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    callable.call(connection).register(new SingleResultCallback<T>() {
                        @Override
                        public void onResult(final T result, final MongoException e) {
                            try {
                                if (e != null) {
                                    future.init(null, e);
                                } else {
                                    future.init(result, null);
                                }
                            } finally {
                                connection.release();
                                source.release();
                            }
                        }
                    });
                }
            }
        });
    }

    static <T> void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource<T> callable,
                                         final SingleResultFuture<T> future) {
        source.getConnection().register(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    callable.call(source, connection).register(new SingleResultCallback<T>() {
                        @Override
                        public void onResult(final T result, final MongoException e) {
                            try {
                                if (e != null) {
                                    future.init(null, e);
                                } else {
                                    future.init(result, null);
                                }
                            } finally {
                                connection.release();
                                source.release();
                            }
                        }
                    });
                }
            }
        });
    }

    static <T> MongoFuture<Void> ignoreResult(final MongoFuture<T> future) {
        return transformResult(future, new VoidTransformer<T>());
    }

    static <T, V> MongoFuture<V> transformResult(final MongoFuture<T> future, final Function<T, V> block) {
        final SingleResultFuture<V> transformedFuture = new SingleResultFuture<V>();
        future.register(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final MongoException e) {
                if (e != null) {
                    transformedFuture.init(null, e);
                } else {
                    transformedFuture.init(block.apply(result), null);
                }
            }
        });
        return transformedFuture;
    }


    private static class AsyncCallableWithConnectionCallback<T> implements SingleResultCallback<AsyncConnectionSource> {
        private final SingleResultFuture<T> future;
        private final AsyncCallableWithConnection<T> callable;

        public AsyncCallableWithConnectionCallback(final SingleResultFuture<T> future, final AsyncCallableWithConnection<T> callable) {
            this.future = future;
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                withConnectionSource(source, callable, future);
            }
        }
    }


    private static class AsyncCallableWithConnectionAndSourceCallback<T> implements SingleResultCallback<AsyncConnectionSource> {
        private final SingleResultFuture<T> future;
        private final AsyncCallableWithConnectionAndSource<T> callable;

        public AsyncCallableWithConnectionAndSourceCallback(final SingleResultFuture<T> future,
                                                            final AsyncCallableWithConnectionAndSource<T> callable) {
            this.future = future;
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                withConnectionSource(source, callable, future);
            }
        }
    }


    private OperationHelper() {
    }
}

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

package com.mongodb.async.client;

import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.connection.Cluster;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.operation.GetDatabaseNamesOperation;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.RootCodecRegistry;

import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.wrapCallback;
import static java.util.Arrays.asList;

class MongoClientImpl implements MongoClient {
    private final Cluster cluster;
    private final MongoClientOptions options;
    private final AsyncOperationExecutor executor;
    private final OperationOptions operationOptions;

    private static final RootCodecRegistry DEFAULT_CODEC_REGISTRY = new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                                 new DocumentCodecProvider(),
                                                                                                 new BsonValueCodecProvider()));

    /**
     * Gets the default codec registry.  It includes the following providers:
     *
     * <ul>
     *     <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     *     <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     *     <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     * @see MongoClientOptions#getCodecRegistry()
     * @since 3.0
     */
    public static RootCodecRegistry getDefaultCodecRegistry() {
        return DEFAULT_CODEC_REGISTRY;
    }

    MongoClientImpl(final MongoClientOptions options, final Cluster cluster) {
        this(options, cluster, createOperationExecutor(options, cluster));
    }

    MongoClientImpl(final MongoClientOptions options, final Cluster cluster, final AsyncOperationExecutor executor) {
        this.options = notNull("options", options);
        this.cluster = notNull("cluster", cluster);
        this.executor = notNull("executor", executor);
        operationOptions = OperationOptions.builder()
                                           .codecRegistry(options.getCodecRegistry())
                                           .readPreference(options.getReadPreference())
                                           .writeConcern(options.getWriteConcern())
                                           .build();
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return getDatabase(name, OperationOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String name, final OperationOptions options) {
        return new MongoDatabaseImpl(name, options.withDefaults(operationOptions), executor);
    }

    @Override
    public void close() {
        cluster.close();
    }

    @Override
    public MongoClientOptions getOptions() {
        return options;
    }

    @Override
    public void getDatabaseNames(final SingleResultCallback<List<String>> callback) {
        executor.execute(new GetDatabaseNamesOperation(), primary(), callback);
    }

    Cluster getCluster() {
        return cluster;
    }

    private static AsyncOperationExecutor createOperationExecutor(final MongoClientOptions options, final Cluster cluster) {
        return new AsyncOperationExecutor(){
            @Override
            public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference,
                                    final SingleResultCallback<T> callback) {
                final SingleResultCallback<T> wrappedCallback = wrapCallback(callback);
                final AsyncReadBinding binding = getReadWriteBinding(readPreference, options, cluster);
                operation.executeAsync(binding, new SingleResultCallback<T>() {
                    @Override
                    public void onResult(final T result, final Throwable t) {
                        try {
                            wrappedCallback.onResult(result, t);
                        } finally {
                            binding.release();
                        }
                    }
                });
            }

            @Override
            public <T> void execute(final AsyncWriteOperation<T> operation, final SingleResultCallback<T> callback) {
                final AsyncWriteBinding binding = getReadWriteBinding(ReadPreference.primary(), options, cluster);
                operation.executeAsync(binding, new SingleResultCallback<T>() {
                    @Override
                    public void onResult(final T result, final Throwable t) {
                        try {
                            callback.onResult(result, t);
                        } finally {
                            binding.release();
                        }
                    }
                });
            }
        };
    }

    private static AsyncReadWriteBinding getReadWriteBinding(final ReadPreference readPreference, final MongoClientOptions options,
                                                             final Cluster cluster) {
        notNull("readPreference", readPreference);
        return new AsyncClusterBinding(cluster, readPreference);
    }
}

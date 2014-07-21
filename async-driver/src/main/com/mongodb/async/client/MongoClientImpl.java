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

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.client.MongoClientOptions;
import com.mongodb.client.MongoDatabaseOptions;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.operation.SingleResultFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class MongoClientImpl implements MongoClient {
    private final Cluster cluster;
    private final MongoClientOptions options;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    MongoClientImpl(final MongoClientOptions options, final Cluster cluster) {
        this.options = options;
        this.cluster = cluster;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(name, this, MongoDatabaseOptions.builder().build().withDefaults(options));
    }

    @Override
    public void close() {
        cluster.close();
    }

    @Override
    public ClientAdministration tools() {
        return new ClientAdministrationImpl(this);
    }

    <V> MongoFuture<V> execute(final AsyncWriteOperation<V> writeOperation) {
        final SingleResultFuture<V> future = new SingleResultFuture<V>();
        final AsyncWriteBinding binding = getWriteBinding();
        writeOperation.executeAsync(binding).register(new SingleResultCallback<V>() {
            @Override
            public void onResult(final V result, final MongoException e) {
                try {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(result, null);
                    }
                } finally {
                    binding.release();
                }
            }
        });
        return future;
    }

    <V> MongoFuture<V> execute(final AsyncReadOperation<V> readOperation, final ReadPreference readPreference) {
        final SingleResultFuture<V> future = new SingleResultFuture<V>();
        final AsyncReadBinding binding = getReadBinding(readPreference);
        readOperation.executeAsync(binding).register(new SingleResultCallback<V>() {
            @Override
            public void onResult(final V result, final MongoException e) {
                try {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(result, null);
                    }
                } finally {
                    binding.release();
                }
            }
        });
        return future;
    }

    private AsyncWriteBinding getWriteBinding() {
        return getReadWriteBinding(ReadPreference.primary());
    }
    private AsyncReadBinding getReadBinding(final ReadPreference readPreference) {
        return getReadWriteBinding(readPreference);
    }

    private AsyncReadWriteBinding getReadWriteBinding(final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return new AsyncClusterBinding(cluster, readPreference, options.getMaxWaitTime(), TimeUnit.MILLISECONDS);
    }
}

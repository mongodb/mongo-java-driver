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

package com.mongodb.client;

import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.ReadPreference.primary;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoClientImpl implements MongoClient {

    private final Cluster cluster;
    private final MongoClientOptions clientOptions;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    MongoClientImpl(final MongoClientOptions clientOptions, final Cluster cluster) {
        this.clientOptions = clientOptions;
        this.cluster = cluster;
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions options) {
        return new MongoDatabaseImpl(databaseName, this, options.withDefaults(clientOptions));
    }

    @Override
    public void close() {
        cluster.close();
        executorService.shutdownNow();
    }

    @Override
    public MongoClientOptions getOptions() {
        return clientOptions;
    }

    @Override
    public ClientAdministration tools() {
        return new ClientAdministrationImpl(this);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Executor getExecutor() {
        return executorService;
    }

    public <V> V execute(final ReadOperation<V> readOperation, final ReadPreference readPreference) {
        ClusterBinding binding = new ClusterBinding(cluster, readPreference, clientOptions.getMaxWaitTime(), MILLISECONDS);
        try {
            return readOperation.execute(binding);
        } finally {
            binding.release();
        }
    }

    public <V> V execute(final WriteOperation<V> writeOperation) {
        ClusterBinding binding = new ClusterBinding(cluster, primary(), clientOptions.getMaxWaitTime(), MILLISECONDS);
        try {
            return writeOperation.execute(binding);
        } finally {
            binding.release();
        }
    }
}

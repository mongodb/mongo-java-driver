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

package com.mongodb;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private final String clusterId;
    private ServerSettings settings;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Mongo mongo;

    public DefaultClusterableServerFactory(final String clusterId, final ServerSettings settings,
                                           final ScheduledExecutorService scheduledExecutorService,
                                           final Mongo mongo) {
        this.clusterId = clusterId;
        this.settings = settings;
        this.scheduledExecutorService = scheduledExecutorService;
        this.mongo = mongo;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        MongoOptions options = mongo.getMongoOptions();
        ConnectionPoolSettings connectionPoolSettings =
        ConnectionPoolSettings.builder()
                              .minSize(options.minConnectionsPerHost)
                              .maxSize(options.getConnectionsPerHost())
                              .maxConnectionIdleTime(options.maxConnectionIdleTime, MILLISECONDS)
                              .maxConnectionLifeTime(options.maxConnectionLifeTime, MILLISECONDS)
                              .maxWaitQueueSize(options.getConnectionsPerHost() * options.getThreadsAllowedToBlockForConnectionMultiplier())
                              .maxWaitTime(options.getMaxWaitTime(), MILLISECONDS)
                              .build();
        return new DefaultServer(serverAddress, settings,
                                 new PooledConnectionProvider(clusterId, serverAddress, new DBPortFactory(options), connectionPoolSettings,
                                                              new JMXConnectionPoolListener(mongo.getMongoOptions().getDescription())),
                                 scheduledExecutorService, mongo);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
    }
}

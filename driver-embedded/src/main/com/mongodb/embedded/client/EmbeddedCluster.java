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

package com.mongodb.embedded.client;

import com.mongodb.MongoConfigurationException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.embedded.capi.MongoEmbeddedLibrary;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Server;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonTimestamp;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

@SuppressWarnings("deprecation")
final class EmbeddedCluster implements Cluster {

    private static final Logger LOGGER = Loggers.getLogger("cluster");
    private final ClusterSettings clusterSettings;
    private final ClusterDescription clusterDescription;
    private final EmbeddedServer server;
    private volatile boolean isClosed;

    EmbeddedCluster(final MongoEmbeddedLibrary mongoEmbeddedLibrary, final MongoClientSettings mongoClientSettings) {
        this.server = new EmbeddedServer(mongoEmbeddedLibrary, mongoClientSettings);
        this.clusterSettings = ClusterSettings.builder().hosts(singletonList(new ServerAddress())).build();
        this.clusterDescription = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE,
                singletonList(server.getDescription()));
    }

    @Override
    public ClusterSettings getSettings() {
        return clusterSettings;
    }

    @Override
    public ClusterDescription getDescription() {
        return clusterDescription;
    }

    @Override
    public ClusterDescription getCurrentDescription() {
        return clusterDescription;
    }

    @Override
    public BsonTimestamp getClusterTime() {
        return null;
    }

    @Override
    public Server selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        List<ServerDescription> servers = serverSelector.select(clusterDescription);
        if (!servers.isEmpty()) {
            return server;
        } else {
            if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(format("No server chosen by %s from cluster description %s.", serverSelector, clusterDescription));
            }
            throw new MongoConfigurationException(format("No server that matches %s. Client view of cluster state is %s",
                    serverSelector, clusterDescription.getShortDescription()));
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<Server> callback) {
        throw new UnsupportedOperationException("Async not supported");
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            server.close();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

}

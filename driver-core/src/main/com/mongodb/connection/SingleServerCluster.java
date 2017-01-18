/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;

import java.util.Arrays;
import java.util.Collections;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.lang.String.format;

/**
 * This class needs to be final because we are leaking a reference to "this" from the constructor
 */
final class SingleServerCluster extends BaseCluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ClusterableServer server;

    SingleServerCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        super(clusterId, settings, serverFactory);
        isTrue("one server in a direct cluster", settings.getHosts().size() == 1);
        isTrue("connection mode is single", settings.getMode() == ClusterConnectionMode.SINGLE);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));
        }

        // synchronized in the constructor because the change listener is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        synchronized (this) {
            this.server = createServer(settings.getHosts().get(0), new ServerListener() {
                @Override
                public void serverOpening(final ServerOpeningEvent event) {
                }

                @Override
                public void serverClosed(final ServerClosedEvent event) {
                }

                @Override
                public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
                    ServerDescription descriptionToPublish = event.getNewDescription();
                    if (event.getNewDescription().isOk()) {
                        if (getSettings().getRequiredClusterType() != ClusterType.UNKNOWN
                            && getSettings().getRequiredClusterType() != event.getNewDescription().getClusterType()) {
                            descriptionToPublish = null;
                        } else if (getSettings().getRequiredClusterType() == ClusterType.REPLICA_SET
                                   && getSettings().getRequiredReplicaSetName() != null) {
                            if (!getSettings().getRequiredReplicaSetName().equals(event.getNewDescription().getSetName())) {
                                descriptionToPublish = null;
                            }
                        }
                    }
                    publishDescription(descriptionToPublish);
                }

            });
            publishDescription(server.getDescription());
        }
    }

    @Override
    protected void connect() {
        server.connect();
    }

    private void publishDescription(final ServerDescription serverDescription) {
        ClusterType clusterType = getSettings().getRequiredClusterType();
        if (clusterType == ClusterType.UNKNOWN && serverDescription != null) {
            clusterType = serverDescription.getClusterType();
        }
        ClusterDescription oldDescription = getCurrentDescription();
        ClusterDescription description = new ClusterDescription(ClusterConnectionMode.SINGLE, clusterType,
                                                                serverDescription == null ? Collections.<ServerDescription>emptyList()
                                                                                          : Arrays.asList(serverDescription),
                                                                       getSettings(), getServerFactory().getSettings());

        updateDescription(description);
        fireChangeEvent(new ClusterDescriptionChangedEvent(getClusterId(), description,
                oldDescription == null ? getInitialDescription() : oldDescription));
    }

    private ClusterDescription getInitialDescription() {
        return new ClusterDescription(getSettings().getMode(), getSettings().getRequiredClusterType(),
                Collections.<ServerDescription>emptyList(), getSettings(), getServerFactory().getSettings());
    }

    @Override
    protected ClusterableServer getServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        return server;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            server.close();
            super.close();
        }
    }
}

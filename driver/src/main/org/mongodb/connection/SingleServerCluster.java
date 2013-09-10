/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.mongodb.diagnostics.Loggers;
import org.mongodb.event.ClusterListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.assertions.Assertions.isTrue;

/**
 * This class needs to be final because we are leaking a reference to "this" from the constructor
 */
final class SingleServerCluster extends BaseCluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ClusterableServer server;

    public SingleServerCluster(final String clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                               final ClusterListener clusterListener) {
        super(clusterId, settings, serverFactory, clusterListener);
        isTrue("one server in a direct cluster", settings.getHosts().size() == 1);
        isTrue("connection mode is single", settings.getMode() == ClusterConnectionMode.Single);

        LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));

        // synchronized in the constructor because the change listener is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        synchronized (this) {
            this.server = createServer(settings.getHosts().get(0), new ChangeListener<ServerDescription>() {
                @Override
                public void stateChanged(final ChangeEvent<ServerDescription> event) {
                    ServerDescription descriptionToPublish = event.getNewValue();
                    if (event.getNewValue().isOk()) {
                        if (getSettings().getRequiredClusterType() != ClusterType.Unknown
                                && getSettings().getRequiredClusterType() != event.getNewValue().getClusterType()) {
                            descriptionToPublish = null;
                        }
                        else if (getSettings().getRequiredClusterType() == ClusterType.ReplicaSet
                                && getSettings().getRequiredReplicaSetName() != null) {
                            if (!getSettings().getRequiredReplicaSetName().equals(event.getNewValue().getSetName())) {
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

    private void publishDescription(final ServerDescription serverDescription) {
        ClusterType clusterType = getSettings().getRequiredClusterType();
        if (clusterType == ClusterType.Unknown && serverDescription != null) {
            clusterType = serverDescription.getClusterType();
        }
        ClusterDescription description = new ClusterDescription(ClusterConnectionMode.Single, clusterType,
                serverDescription == null ? Collections.<ServerDescription>emptyList() : Arrays.asList(serverDescription));

        ClusterDescription oldDescription = getDescriptionNoWaiting();
        updateDescription(description);
        if (oldDescription != null) {
            fireChangeEvent(new ChangeEvent<ClusterDescription>(oldDescription, getDescriptionNoWaiting()));
        }
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

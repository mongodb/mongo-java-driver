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

/**
 * A factory for cluster implementations.
 */
final class Clusters {
    private Clusters() {
    }

    public static Cluster create(final String clusterId, final ClusterSettings settings, final ServerSettings serverSettings,
                                 final ClusterListener clusterListener, final Mongo mongo) {
        ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(clusterId, serverSettings, mongo);

        if (settings.getMode() == ClusterConnectionMode.Single) {
            return new SingleServerCluster(clusterId, settings, serverFactory,
                                           clusterListener != null ? clusterListener : new NoOpClusterListener());
        } else if (settings.getMode() == ClusterConnectionMode.Multiple) {
            return new MultiServerCluster(clusterId, settings, serverFactory,
                                          clusterListener != null ? clusterListener : new NoOpClusterListener());
        } else {
            throw new UnsupportedOperationException("Unsupported cluster mode: " + settings.getMode());
        }
    }
}
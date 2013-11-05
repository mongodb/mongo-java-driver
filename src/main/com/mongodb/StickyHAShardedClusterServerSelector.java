/*
 * Copyright (c) 2008 - 2013 MongoDB Inc., Inc. <http://mongodb.com>
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class StickyHAShardedClusterServerSelector implements StatefulServerSelector {
    private ServerAddress stickyMongos;
    private Set<ServerAddress> consideredServers = new HashSet<ServerAddress>();

    @Override
    public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() != ClusterConnectionMode.Multiple
            || clusterDescription.getType() != ClusterType.Sharded) {
            throw new IllegalArgumentException("This is not a sharded cluster with multiple mongos servers");
        }

        Set<ServerAddress> okServers = getOkServers(clusterDescription);

        synchronized (this) {
            if (!consideredServers.containsAll(okServers) || !okServers.contains(stickyMongos)) {
                if (!okServers.contains(stickyMongos)) {
                    stickyMongos = null;
                    consideredServers.clear();
                }
                ServerDescription fastestServer = null;
                for (ServerDescription cur : clusterDescription.getAny()) {
                    if (fastestServer == null || cur.getAveragePingTimeNanos() < fastestServer.getAveragePingTimeNanos()) {
                        fastestServer = cur;
                    }
                }
                if (fastestServer != null) {
                    stickyMongos = fastestServer.getAddress();
                    consideredServers.addAll(okServers);
                }
            }
            if (stickyMongos == null) {
                return Collections.emptyList();
            } else {
                return Arrays.asList(clusterDescription.getByServerAddress(stickyMongos));
            }
        }
    }

    public synchronized void clear() {
        stickyMongos = null;
    }

    @Override
    public String toString() {
        return "StickyHAShardedClusterServerSelector{"
               + "stickyMongos=" + stickyMongos
               + '}';
    }

    private Set<ServerAddress> getOkServers(final ClusterDescription clusterDescription) {
        Set<ServerAddress> okServers = new HashSet<ServerAddress>();
        for (ServerDescription cur : clusterDescription.getAny()) {
            okServers.add(cur.getAddress());
        }
        return okServers;
    }
}

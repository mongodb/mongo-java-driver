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

package com.mongodb.internal.connection;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class DnsMultiServerCluster extends AbstractMultiServerCluster {
    private final DnsSrvRecordMonitor dnsSrvRecordMonitor;
    private volatile MongoException srvResolutionException;

    public DnsMultiServerCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                                 final DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory) {
        super(clusterId, settings, serverFactory);
        dnsSrvRecordMonitor = dnsSrvRecordMonitorFactory.create(assertNotNull(settings.getSrvHost()), settings.getSrvServiceName(),
                new DnsSrvRecordInitializer() {
            private volatile boolean initialized;

            @Override
            public void initialize(final Collection<ServerAddress> hosts) {
                srvResolutionException = null;
                if (!initialized) {
                    initialized = true;
                    DnsMultiServerCluster.this.initialize(applySrvMaxHosts(hosts));
                } else {
                    DnsMultiServerCluster.this.onChange(applySrvMaxHosts(hosts));
                }
            }

            private Collection<ServerAddress> applySrvMaxHosts(final Collection<ServerAddress> latestSrvHosts) {
                Integer srvMaxHosts = getSettings().getSrvMaxHosts();
                if (srvMaxHosts == null || srvMaxHosts <= 0 || latestSrvHosts.size() <= srvMaxHosts) {
                    return new ArrayList<>(latestSrvHosts);
                }
                List<ServerAddress> activePriorHosts = getActivePriorHosts(latestSrvHosts);
                int numNewHostsToAdd = srvMaxHosts - activePriorHosts.size();
                List<ServerAddress> result = getShuffledLatestSrvHosts(latestSrvHosts, activePriorHosts, numNewHostsToAdd);

                return result;
            }

            private List<ServerAddress> getActivePriorHosts(Collection<ServerAddress> latestSrvHosts) {
                List<ServerAddress> priorHosts = DnsMultiServerCluster.this.getCurrentDescription().getServerDescriptions().stream()
                        .map(ServerDescription::getAddress).collect(Collectors.toList());
                priorHosts.removeIf(host -> !latestSrvHosts.contains(host));

                return priorHosts;
            }

            private List<ServerAddress> getShuffledLatestSrvHosts(final Collection<ServerAddress> latestSrvHosts,
                    List<ServerAddress> activePriorHosts, int numNewHostsToAdd) {
                List<ServerAddress> addedHosts = new ArrayList<>(latestSrvHosts);
                addedHosts.removeAll(activePriorHosts);
                Collections.shuffle(addedHosts, ThreadLocalRandom.current());
                // add the shuffled latestSrvHosts to the activePriorHosts
                activePriorHosts.addAll(addedHosts.subList(0, numNewHostsToAdd));

                return activePriorHosts;
            }

            @Override
            public void initialize(final MongoException initializationException) {
                if (!initialized) {
                    srvResolutionException = initializationException;
                    DnsMultiServerCluster.this.initialize(Collections.emptyList());
                }
            }

            @Override
            public ClusterType getClusterType() {
                return DnsMultiServerCluster.this.getClusterType();
            }
        });
        dnsSrvRecordMonitor.start();
    }

    @Nullable
    @Override
    protected MongoException getSrvResolutionException() {
        return srvResolutionException;
    }

    @Override
    public void close() {
        if (dnsSrvRecordMonitor != null) {
            dnsSrvRecordMonitor.close();
        }
        super.close();
    }
}

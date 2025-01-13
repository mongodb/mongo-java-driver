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

            private Collection<ServerAddress> applySrvMaxHosts(final Collection<ServerAddress> newHosts) {
                Integer srvMaxHosts = getSettings().getSrvMaxHosts();
                if (srvMaxHosts == null || srvMaxHosts <= 0 || newHosts.size() <= srvMaxHosts) {
                    return new ArrayList<>(newHosts);
                }
                // prior hosts
                List<ServerAddress> result = DnsMultiServerCluster.this.getCurrentDescription().getServerDescriptions().stream()
                        .map(ServerDescription::getAddress).collect(Collectors.toList());
                int numPriorHosts = result.size();

                result.removeIf(host -> !newHosts.contains(host));
                int numRemovedHosts = numPriorHosts - result.size();
                int numNewHostsToAdd = srvMaxHosts - numPriorHosts + numRemovedHosts;

                List<ServerAddress> addedHosts = new ArrayList<>(newHosts);
                addedHosts.removeAll(result);
                Collections.shuffle(addedHosts, ThreadLocalRandom.current());
                // add the select shuffled list to the priorHosts that are a part of the newHosts
                result.addAll(addedHosts.subList(0, numNewHostsToAdd));

                return result;
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

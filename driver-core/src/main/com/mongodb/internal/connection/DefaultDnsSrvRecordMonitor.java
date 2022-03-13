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
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.connection.dns.DnsResolver;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.mongodb.internal.connection.ServerAddressHelper.createServerAddress;
import static java.util.Collections.unmodifiableSet;

class DefaultDnsSrvRecordMonitor implements DnsSrvRecordMonitor {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final String hostName;
    private final String srvServiceName;
    private final long rescanFrequencyMillis;
    private final long noRecordsRescanFrequencyMillis;
    private final DnsSrvRecordInitializer dnsSrvRecordInitializer;
    private final DnsResolver dnsResolver;
    private final Thread monitorThread;
    private volatile boolean isClosed;

    DefaultDnsSrvRecordMonitor(final String hostName, final String srvServiceName, final long rescanFrequencyMillis, final long noRecordsRescanFrequencyMillis,
            final DnsSrvRecordInitializer dnsSrvRecordInitializer, final ClusterId clusterId,
            final DnsResolver dnsResolver) {
        this.hostName = hostName;
        this.srvServiceName = srvServiceName;
        this.rescanFrequencyMillis = rescanFrequencyMillis;
        this.noRecordsRescanFrequencyMillis = noRecordsRescanFrequencyMillis;
        this.dnsSrvRecordInitializer = dnsSrvRecordInitializer;
        this.dnsResolver = dnsResolver;
        monitorThread = new Thread(new DnsSrvRecordMonitorRunnable(), "cluster-" + clusterId + "-srv-" + hostName);
        monitorThread.setDaemon(true);
    }

    @Override
    public void start() {
        monitorThread.start();
    }

    @Override
    public void close() {
        isClosed = true;
        monitorThread.interrupt();
    }

    private class DnsSrvRecordMonitorRunnable implements Runnable {
        private Set<ServerAddress> currentHosts = Collections.emptySet();
        private ClusterType clusterType = ClusterType.UNKNOWN;

        @Override
        public void run() {
            while (!isClosed && shouldContinueMonitoring()) {
                try {
                    List<String> resolvedHostNames = dnsResolver.resolveHostFromSrvRecords(hostName, srvServiceName);
                    Set<ServerAddress> hosts = createServerAddressSet(resolvedHostNames);

                    if (isClosed) {
                        return;
                    }

                    if (!hosts.equals(currentHosts)) {
                        try {
                            dnsSrvRecordInitializer.initialize(unmodifiableSet(hosts));
                            currentHosts = hosts;
                        } catch (RuntimeException e) {
                            LOGGER.warn("Exception in monitor thread during notification of DNS resolution state change", e);
                        }
                    }
                } catch (MongoException e) {
                    if (currentHosts.isEmpty()) {
                        dnsSrvRecordInitializer.initialize(e);
                    }
                    LOGGER.info("Exception while resolving SRV records", e);
                } catch (RuntimeException e) {
                    if (currentHosts.isEmpty()) {
                        dnsSrvRecordInitializer.initialize(new MongoInternalException("Unexpected runtime exception", e));
                    }
                    LOGGER.info("Unexpected runtime exception while resolving SRV record", e);
                }

                try {
                    Thread.sleep(getRescanFrequencyMillis());
                } catch (InterruptedException e) {
                    // fall through
                }
                clusterType = dnsSrvRecordInitializer.getClusterType();
            }
        }

        private boolean shouldContinueMonitoring() {
            return clusterType == ClusterType.UNKNOWN || clusterType == ClusterType.SHARDED;
        }

        private long getRescanFrequencyMillis() {
            return currentHosts.isEmpty() ? noRecordsRescanFrequencyMillis : rescanFrequencyMillis;
        }

        private Set<ServerAddress> createServerAddressSet(final List<String> resolvedHostNames) {
            Set<ServerAddress> hosts = new HashSet<ServerAddress>(resolvedHostNames.size());
            for (String host : resolvedHostNames) {
                hosts.add(createServerAddress(host));
            }
            return hosts;
        }
    }
}


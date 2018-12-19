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

import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.internal.dns.DefaultDnsResolver;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DefaultDnsSrvRecordMonitorFactory implements DnsSrvRecordMonitorFactory {

    // JNDI doesn't provide the TTL for DNS records, so we have to hard-code it
    private static final long DEFAULT_RESCAN_FREQUENCY_MILLIS = 60000;

    private final ClusterId clusterId;
    private final long noRecordsRescanFrequency;

    public DefaultDnsSrvRecordMonitorFactory(final ClusterId clusterId, final ServerSettings serverSettings) {
        this.clusterId = clusterId;
        this.noRecordsRescanFrequency = serverSettings.getHeartbeatFrequency(MILLISECONDS);
    }

    @Override
    public DnsSrvRecordMonitor create(final String hostName, final DnsSrvRecordInitializer dnsSrvRecordInitializer) {
        return new DefaultDnsSrvRecordMonitor(hostName, DEFAULT_RESCAN_FREQUENCY_MILLIS, noRecordsRescanFrequency, dnsSrvRecordInitializer,
                clusterId, new DefaultDnsResolver());
    }
}

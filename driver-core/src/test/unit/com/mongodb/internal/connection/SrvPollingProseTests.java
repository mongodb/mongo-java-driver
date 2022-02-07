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

import com.mongodb.MongoConfigurationException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.internal.dns.DnsResolver;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.naming.NamingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SrvPollingProseTests {

    private final ClusterId clusterId = new ClusterId();
    private final String srvHost = "test1.test.build.10gen.cc";
    private final String srvServiceName = "mongodb";
    private final ClusterSettings.Builder settingsBuilder = ClusterSettings.builder()
            .mode(ClusterConnectionMode.MULTIPLE)
            .requiredClusterType(ClusterType.SHARDED)
            .srvHost(srvHost);
    private final ClusterableServerFactory serverFactory = mock(ClusterableServerFactory.class);
    private final String firstHost = "localhost.test.build.10gen.cc:27017";
    private final String secondHost = "localhost.test.build.10gen.cc:27018";
    private final String thirdHost = "localhost.test.build.10gen.cc:27019";
    private final String fourthHost = "localhost.test.build.10gen.cc:27020";

    private final List<String> initialHosts = asList(firstHost, secondHost);
    private DnsSrvRecordMonitor dnsSrvRecordMonitor;
    private DnsMultiServerCluster cluster;

    @BeforeEach
    public void beforeEach() {
        when(serverFactory.getSettings()).thenReturn(ServerSettings.builder().build());
        when(serverFactory.create(any(), any())).thenReturn(mock(ClusterableServer.class));
    }

    @AfterEach
    public void afterEach() {
        if (cluster != null) {
            cluster.close();
        }
    }

    // 1. Addition of a new DNS record
    @Test
    public void shouldAddDnsRecord() {
        List<String> updatedHosts = asList(firstHost, secondHost, thirdHost);

        initCluster(updatedHosts);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 2. Removal of an existing DNS record
    @Test
    public void shouldRemoveDnsRecord() {
        List<String> updatedHosts = asList(firstHost);

        initCluster(updatedHosts);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 3. Replacement of a DNS record
    @Test
    public void shouldReplaceDnsRecord() {
        List<String> updatedHosts = asList(firstHost, thirdHost);

        initCluster(updatedHosts);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 4. Replacement of both existing DNS records with *one* new record
    @Test
    public void shouldReplaceTwoDnsRecordsWithOne() {
        List<String> updatedHosts = asList(thirdHost);

        initCluster(updatedHosts);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 5. Replacement of both existing DNS records with *two* new records
    @Test
    public void shouldReplaceTwoDnsRecordsWithTwoNewOnes() {
        List<String> updatedHosts = asList(thirdHost, fourthHost);

        initCluster(updatedHosts);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 6. DNS record lookup timeout
    // Unimplemented as DnsResolver doesn't throw a different exception for timeouts

    // 7. DNS record lookup failure
    @Test
    public void shouldIgnoreDnsRecordLookupFailure() {
        initCluster(new MongoConfigurationException("Unable to look up SRV record for host " + srvHost, new NamingException()));
        assertEquals(setOf(initialHosts), clusterHostsSet());
    }

    // 8. Removal of all DNS SRV records
    // Unimplemented: the test is supposed to assert that the hosts are unchanged, but the driver actually changes them

    // 9. Test that SRV polling is not done for load balanced clusters
    // Unimplemented because DnsMultiServerCluster is not used for load balanced clusters

    // 10. All DNS records are selected (srvMaxHosts = 0)
    @Test
    public void shouldUseAllRecordsWhenSrvMaxHostsIsZero() {
        List<String> updatedHosts = asList(firstHost, thirdHost, fourthHost);

        initCluster(updatedHosts, 0);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 11. All DNS records are selected (srvMaxHosts >= records)
    @Test
    public void shouldUseAllRecordsWhenSrvMaxHostsIsGreaterThanOrEqualToNumSrvRecords() {
        List<String> updatedHosts = asList(thirdHost, fourthHost);

        initCluster(updatedHosts, 2);
        assertEquals(setOf(updatedHosts), clusterHostsSet());
    }

    // 12. New DNS records are randomly selected (srvMaxHosts > 0)
    @Test
    public void shouldUseSrvMaxHostsWhenSrvMaxHostsIsLessThanNumSrvRecords() {
        int srvMaxHosts = 2;
        List<String> updatedHosts = asList(firstHost, thirdHost, fourthHost);

        initCluster(updatedHosts, srvMaxHosts);
        assertEquals(srvMaxHosts, clusterHostsSet().size());
        assertTrue(updatedHosts.containsAll(clusterHostsSet()));
    }

    private Set<String> clusterHostsSet() {
        return cluster.getCurrentDescription().getServerDescriptions().stream()
                .map(ServerDescription::getAddress)
                .map(ServerAddress::toString)
                .collect(Collectors.toSet());
    }

    private Set<String> setOf(final List<String> list) {
        return new HashSet<>(list);
    }

    private void initCluster(final List<String> updatedHosts) {
        initCluster(new TestDnsResolver(asList(initialHosts, updatedHosts)), null);
    }

    private void initCluster(final List<String> updatedHosts, @Nullable final Integer srvMaxHosts) {
        initCluster(new TestDnsResolver(asList(initialHosts, updatedHosts)), srvMaxHosts);
    }

    private void initCluster(final RuntimeException lastResponseException) {
        initCluster(new TestDnsResolver(asList(initialHosts), lastResponseException), null);
    }

    private void initCluster(final TestDnsResolver dnsResolver, @Nullable final Integer srvMaxHosts) {
        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHost), eq(srvServiceName), any())).thenAnswer(
                invocation -> {
                    dnsSrvRecordMonitor = new DefaultDnsSrvRecordMonitor(srvHost, srvServiceName, 10, 10,
                            invocation.getArgument(2), clusterId, dnsResolver);
                    return dnsSrvRecordMonitor;
                });
        cluster = new DnsMultiServerCluster(clusterId, settingsBuilder.srvMaxHosts(srvMaxHosts).build(), serverFactory,
                dnsSrvRecordMonitorFactory);
        try {
            Thread.sleep(100); // racy
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static final class TestDnsResolver implements DnsResolver {
        private final List<List<String>> responses;
        private final RuntimeException lastResponseException;
        private int curPos = 0;

        TestDnsResolver(final List<List<String>> responses) {
            this(responses, null);
        }

        TestDnsResolver(final List<List<String>> responses, final RuntimeException lastResponseException) {
            this.responses = responses;
            this.lastResponseException = lastResponseException;
        }

        @Override
        public List<String> resolveHostFromSrvRecords(final String srvHost, final String srvServiceName) {
            List<String> retVal;
            if (curPos >= responses.size() && lastResponseException != null) {
                throw lastResponseException;
            } else if (curPos >= responses.size() - 1) {
                retVal = responses.get(responses.size() - 1);
            } else {
                retVal = responses.get(curPos);
            }
            curPos++;
            return retVal;
        }

        @Override
        public String resolveAdditionalQueryParametersFromTxtRecords(final String host) {
            throw new UnsupportedOperationException();
        }
    }
}

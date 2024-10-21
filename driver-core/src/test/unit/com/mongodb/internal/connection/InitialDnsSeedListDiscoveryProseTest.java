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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerSettings;
import com.mongodb.internal.dns.DefaultDnsResolver;
import com.mongodb.internal.dns.DnsResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * See https://github.com/mongodb/specifications/blob/master/source/initial-dns-seedlist-discovery/tests/README.md
 */
class InitialDnsSeedListDiscoveryProseTest {
    private static final String SRV_SERVICE_NAME = "mongodb";

    private DnsMultiServerCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @ParameterizedTest(name = "mongodb+srv://{0} => {1}")
    @CsvSource({
            "localhost, mongo.localhost",
            "mongo.local, driver.mongo.local"
    })
    @DisplayName("1. Allow SRVs with fewer than 3 '.' separated parts")
    void test1(final String srvHost, final String resolvedHost) {
        doTest(srvHost, resolvedHost, false);
    }

    @ParameterizedTest(name = "mongodb+srv://{0} => {1}")
    @CsvSource({
            "localhost, localhost.mongodb",
            "mongo.local, test_1.evil.local",
            "blogs.mongodb.com, blogs.evil.com"
    })
    @DisplayName("2. Throw when return address does not end with SRV domain")
    void test2(final String srvHost, final String resolvedHost) {
        doTest(srvHost, resolvedHost, true);
    }

    @ParameterizedTest(name = "mongodb+srv://{0} => {1}")
    @CsvSource({
            "localhost, localhost",
            "mongo.local, mongo.local"
    })
    @DisplayName("3. Throw when return address is identical to SRV hostname and return address does not contain '.' separating shared part of domain")
    void test3(final String srvHost, final String resolvedHost) {
        doTest(srvHost, resolvedHost, true);
    }

    @ParameterizedTest(name = "mongodb+srv://{0} => {1}")
    @CsvSource({
            "localhost, test_1.cluster_1localhost",
            "mongo.local, test_1.my_hostmongo.local",
            "blogs.mongodb.com, cluster.testmongodb.com"
    })
    @DisplayName("4. Throw when return address does not contain '.' separating shared part of domain")
    void test4(final String srvHost, final String resolvedHost) {
        doTest(srvHost, resolvedHost, true);
    }

    private void doTest(final String srvHost, final String resolvedHost, final boolean throwException) {
        final ClusterId clusterId = new ClusterId();

        final DnsResolver dnsResolver = new DefaultDnsResolver((name, type) -> singletonList(String.format("10 5 27017 %s",
                resolvedHost)));

        final DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHost), eq(SRV_SERVICE_NAME), any(DnsSrvRecordInitializer.class))).thenAnswer(
                invocation -> new DefaultDnsSrvRecordMonitor(srvHost, SRV_SERVICE_NAME, 10, 10,
                            invocation.getArgument(2), clusterId, dnsResolver));

        final ClusterSettings.Builder settingsBuilder = ClusterSettings.builder()
                .mode(ClusterConnectionMode.MULTIPLE)
                .requiredClusterType(ClusterType.SHARDED)
                .srvHost(srvHost);

        final ClusterableServerFactory serverFactory = mock(ClusterableServerFactory.class);
        when(serverFactory.getSettings()).thenReturn(ServerSettings.builder().build());
        when(serverFactory.create(any(Cluster.class), any(ServerAddress.class))).thenReturn(mock(ClusterableServer.class));

        cluster = new DnsMultiServerCluster(clusterId, settingsBuilder.build(),
                serverFactory,
                dnsSrvRecordMonitorFactory);

        ClusterFixture.sleep(100);

        final MongoException mongoException = cluster.getSrvResolutionException();
        if (throwException) {
            Assertions.assertNotNull(mongoException);
        } else {
            Assertions.assertNull(mongoException);
        }
    }
}


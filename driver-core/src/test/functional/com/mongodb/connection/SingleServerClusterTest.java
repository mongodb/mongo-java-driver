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

package com.mongodb.connection;

import com.mongodb.selector.ServerSelector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getCredentialList;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getSSLSettings;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SingleServerClusterTest {
    private SingleServerCluster cluster;

    @Before
    public void setUp() throws Exception {
        SocketStreamFactory streamFactory = new SocketStreamFactory(SocketSettings.builder().build(),
                                                                    getSSLSettings());
        cluster = new SingleServerCluster("1",
                                          ClusterSettings.builder()
                                                         .mode(ClusterConnectionMode.SINGLE)
                                                         .hosts(Arrays.asList(getPrimary()))
                                                         .build(),
                                          new DefaultClusterableServerFactory("1",
                                                                              ServerSettings.builder().build(),
                                                                              ConnectionPoolSettings.builder().maxSize(1).build(),
                                                                              streamFactory,
                                                                              streamFactory,
                                                                              getCredentialList(),
                                                                              new NoOpConnectionListener(),
                                                                              new NoOpConnectionPoolListener()),
                                          new NoOpClusterListener());
    }

    @After
    public void tearDown() {
        cluster.close();
    }

    @Test
    public void shouldGetDescription() {
        assertNotNull(cluster.getDescription(10, TimeUnit.SECONDS));
    }

    @Test
    public void shouldGetServerWithOkDescription() throws InterruptedException {
        Server server = cluster.selectServer(new ServerSelector() {
            @Override
            public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                return clusterDescription.getPrimaries();
            }
        }, 10, TimeUnit.SECONDS);
        assertTrue(server.getDescription().isOk());
    }

}

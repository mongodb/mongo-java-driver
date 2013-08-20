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

package org.mongodb.connection.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ClusterSettings;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerSelector;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getCredentialList;
import static org.mongodb.Fixture.getPrimary;
import static org.mongodb.Fixture.getSSLSettings;

public class SingleServerClusterTest {
    private SingleServerCluster cluster;

    @Before
    public void setUp() throws Exception {
        cluster = new SingleServerCluster(
                ClusterSettings.builder().mode(ClusterConnectionMode.Single).hosts(Arrays.asList(getPrimary())).build(),
                new DefaultClusterableServerFactory(ServerSettings.builder().build(),
                        new DefaultConnectionProviderFactory(ConnectionProviderSettings.builder().maxSize(1).build(),
                                new DefaultConnectionFactory(ConnectionSettings.builder().build(), getSSLSettings(),
                                        getBufferProvider(), getCredentialList())),
                        null,
                        new DefaultConnectionFactory(ConnectionSettings.builder().build(), getSSLSettings(), getBufferProvider(),
                                getCredentialList()), Executors.newScheduledThreadPool(1), getBufferProvider()));
    }

    @After
    public void tearDown() {
        cluster.close();
    }

    @Test
    public void shouldGetDescription() {
        assertNotNull(cluster.getDescription());
    }

    @Test
    public void shouldGetServerWithOkDescription() throws InterruptedException {
        Server server = cluster.getServer(new ServerSelector() {
            @Override
            public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
                return clusterDescription.getPrimaries();
            }
        });
        assertTrue(server.getDescription().isOk());
    }


}

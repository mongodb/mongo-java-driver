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

package org.mongodb.connection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        cluster = new SingleServerCluster("1",
                ClusterSettings.builder().mode(ClusterConnectionMode.Single).hosts(Arrays.asList(getPrimary())).build(),
                new DefaultClusterableServerFactory("1", ServerSettings.builder().build(),
                        ConnectionPoolSettings.builder().maxSize(1).build(),
                        new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                        new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()), Executors.newScheduledThreadPool(1),
                        getCredentialList(), getBufferProvider(), new NoOpConnectionListener(), new NoOpConnectionPoolListener()),
                new NoOpClusterListener());
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

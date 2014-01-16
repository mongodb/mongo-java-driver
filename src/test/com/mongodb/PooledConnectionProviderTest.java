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

package com.mongodb;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class PooledConnectionProviderTest {
    private static final String CLUSTER_ID = "1";

    private ServerAddress serverAddress;

    private TestInternalConnectionFactory connectionFactory;

    private PooledConnectionProvider provider;

    @Before
    public void setup() throws UnknownHostException {
        serverAddress = new ServerAddress();
        connectionFactory = new TestInternalConnectionFactory();
    }

    @After
    public void cleanup() {
        provider.close();
    }

    @Test
    public void shouldThrowOnTimeout() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress, connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maxWaitTime(50, MILLISECONDS)
                                                                      .build(),
                                                new NoOpConnectionPoolListener());
        provider.get();

        // when
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(provider);
        new Thread(connectionGetter).start();

        connectionGetter.getLatch().await();

        // then
        assertTrue(connectionGetter.isGotTimeout());
    }

    @Test
    public void shouldThrowOnWaitQueueFull() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress, connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maxWaitTime(500, MILLISECONDS)
                                                                      .build(),
                                                new NoOpConnectionPoolListener());

        provider.get();

        new Thread(new TimeoutTrackingConnectionGetter(provider)).start();
        Thread.sleep(100);

        // when
        try {
            provider.get();
            fail();
        } catch (MongoWaitQueueFullException e) {
            // then
            // all good
        }
    }

    @Test
    public void shouldExpireConnectionAfterMaxLifeTime() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress, connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maxConnectionLifeTime(50, MILLISECONDS)
                                                                      .build(),
                                                new NoOpConnectionPoolListener());

        // when
        provider.release(provider.get());
        Thread.sleep(100);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getNumCreatedConnections() >= 2);  // should really be two, but it's racy
    }

    @Test
    public void shouldExpireConnectionAfterLifeTimeOnClose() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maxConnectionLifeTime(20, MILLISECONDS).build(),
                                                new NoOpConnectionPoolListener());

        // when
        Connection connection = provider.get();
        Thread.sleep(50);
        provider.release(connection);

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldExpireConnectionAfterMaxIdleTime() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maintenanceInitialDelay(60, SECONDS)
                                                                      .maxConnectionIdleTime(50, MILLISECONDS)
                                                                      .build(),
                                                new NoOpConnectionPoolListener());

        // when
        provider.release(provider.get());
        Thread.sleep(100);
        provider.doMaintenance();
        provider.get();

        // then
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldCloseConnectionAfterExpiration() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maintenanceInitialDelay(60, SECONDS)
                                                                      .maxConnectionLifeTime(20, MILLISECONDS).build(),
                                                new NoOpConnectionPoolListener());

        // when
        provider.release(provider.get());
        Thread.sleep(50);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldCreateNewConnectionAfterExpiration() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(1)
                                                                      .maxWaitQueueSize(1)
                                                                      .maintenanceInitialDelay(60, SECONDS)
                                                                      .maxConnectionLifeTime(20, MILLISECONDS).build(),
                                                new NoOpConnectionPoolListener());

        // when
        provider.release(provider.get());
        Thread.sleep(50);
        provider.doMaintenance();
        Connection secondConnection = provider.get();

        // then
        assertNotNull(secondConnection);
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldPruneAfterMaintenanceTaskRuns() throws InterruptedException {
        // given
        provider = new PooledConnectionProvider(CLUSTER_ID, serverAddress,
                                                connectionFactory,
                                                ConnectionPoolSettings.builder()
                                                                      .maxSize(10)
                                                                      .maxConnectionLifeTime(1, MILLISECONDS)
                                                                      .maintenanceInitialDelay(60, SECONDS)
                                                                      .maxWaitQueueSize(1)
                                                                      .build(),
                                                new NoOpConnectionPoolListener());
        provider.release(provider.get());

        // when
        Thread.sleep(10);
        provider.doMaintenance();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }
}
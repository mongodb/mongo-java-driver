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

package com.mongodb.connection;

import com.mongodb.MongoWaitQueueFullException;
import com.mongodb.ServerAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * These tests are racy, so doing them in Java instead of Groovy to reduce chance of failure.
 */
public class DefaultConnectionPoolTest {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress());

    private TestInternalConnectionFactory connectionFactory;

    private DefaultConnectionPool provider;

    @Before
    public void setUp() {
        connectionFactory = new TestInternalConnectionFactory();
    }

    @After
    public void cleanup() {
        provider.close();
    }

    @Test
    public void shouldThrowOnTimeout() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maxWaitTime(50, MILLISECONDS)
                                                                   .build());
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
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maxWaitTime(500, MILLISECONDS)
                                                                   .build());

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
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maintenanceInitialDelay(5, MINUTES)
                                                                   .maxConnectionLifeTime(50, MILLISECONDS)
                                                                   .build());

        // when
        provider.get().close();
        Thread.sleep(100);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getNumCreatedConnections() >= 2);  // should really be two, but it's racy
    }

    @Test
    public void shouldExpireConnectionAfterLifeTimeOnClose() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                                             connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maxConnectionLifeTime(20, MILLISECONDS).build());

        // when
        InternalConnection connection = provider.get();
        Thread.sleep(50);
        connection.close();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldExpireConnectionAfterMaxIdleTime() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                                             connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maintenanceInitialDelay(5, MINUTES)
                                                                   .maxConnectionIdleTime(50, MILLISECONDS).build());

        // when
        provider.get().close();
        Thread.sleep(100);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getNumCreatedConnections() >= 2);  // should really be two, but it's racy
    }

    @Test
    public void shouldCloseConnectionAfterExpiration() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                                             connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maintenanceInitialDelay(5, MINUTES)
                                                                   .maxConnectionLifeTime(20, MILLISECONDS).build());

        // when
        provider.get().close();
        Thread.sleep(50);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldCreateNewConnectionAfterExpiration() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                                             connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maintenanceInitialDelay(5, MINUTES)
                                                                   .maxConnectionLifeTime(20, MILLISECONDS).build());

        // when
        provider.get().close();
        Thread.sleep(50);
        provider.doMaintenance();
        InternalConnection secondConnection = provider.get();

        // then
        assertNotNull(secondConnection);
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldPruneAfterMaintenanceTaskRuns() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                                             connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(10)
                                                                   .maxConnectionLifeTime(1, MILLISECONDS)
                                                                   .maintenanceInitialDelay(5, MINUTES)
                                                                   .maxWaitQueueSize(1)
                                                                   .build());
        provider.get().close();

        // when
        Thread.sleep(10);
        provider.doMaintenance();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldNotCallWaitQueueExitedIfWaitQueueEnteredWasNotCalled() throws InterruptedException {
        // given
        QueueEventsConnectionPoolListener listener = new QueueEventsConnectionPoolListener();

        // when
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                                             ConnectionPoolSettings.builder()
                                                                   .maxSize(1)
                                                                   .maxWaitQueueSize(1)
                                                                   .maxWaitTime(5, SECONDS)
                                                                   .addConnectionPoolListener(listener)
                                                                   .build());

        // then
        assertEquals(0, listener.getWaitQueueSize());

        // when
        InternalConnection connection = provider.get();
        TimeoutTrackingConnectionGetter timeoutTrackingConnectionGetter = new TimeoutTrackingConnectionGetter(provider);
        new Thread(timeoutTrackingConnectionGetter).start();
        Thread.sleep(100);

        // then
        try {
            provider.get();
            fail();
        } catch (MongoWaitQueueFullException e) {
            assertEquals(1, listener.getWaitQueueSize());
        }

        // when
        connection.close();
        timeoutTrackingConnectionGetter.getLatch().await();
        connection = provider.get();

        // then
        assertEquals(0, listener.getWaitQueueSize());

        // cleanup
        connection.close();
    }
}

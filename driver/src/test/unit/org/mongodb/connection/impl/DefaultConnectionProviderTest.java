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

import org.bson.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ConnectionProvider;
import org.mongodb.connection.MongoSocketWriteException;
import org.mongodb.connection.MongoTimeoutException;
import org.mongodb.connection.MongoWaitQueueFullException;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mongodb.Fixture.getPrimary;

public class DefaultConnectionProviderTest {

    private DefaultConnectionProvider pool;
    private TestConnectionFactory connectionFactory;

    @Before
    public void setUp() {
        connectionFactory = new TestConnectionFactory();
    }

    @After
    public void tearDown() {
        pool.close();
    }

    @Test
    public void shouldGetNonNullConnection() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        Connection first = pool.get();
        assertNotNull(first);
    }

    @Test
    public void shouldReuseReleasedConnection() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        pool.get().close();
        pool.get();
        assertEquals(1, connectionFactory.getConnections().size());
    }

    @Test
    public void shouldThrowIfPoolIsExhausted() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        Connection first = pool.get();
        assertNotNull(first);

        try {
            pool.get();
            fail();
        } catch (MongoTimeoutException e) {
            // all good
        }
    }

    @Test
    public void shouldThrowOnTimeout() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxWaitTime(50, TimeUnit.MILLISECONDS)
                        .build());

        pool.get();

        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(pool);
        new Thread(connectionGetter).start();

        connectionGetter.latch.await();

        assertTrue(connectionGetter.gotTimeout);
    }

    @Test
    public void shouldThrowOnWaitQueueFull() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxWaitTime(1000, TimeUnit.MILLISECONDS)
                        .build());

        pool.get();

        TimeoutTrackingConnectionGetter timeoutTrackingGetter = new TimeoutTrackingConnectionGetter(pool);
        Thread.sleep(100);
        new Thread(timeoutTrackingGetter).start();
        try {
            pool.get();
            fail();
        } catch (MongoWaitQueueFullException e){
            //all good
        }
    }

    @Test
    public void shouldExpireConnectionAfterMaxLifeTime() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(5, TimeUnit.MILLISECONDS).build());
        pool.get().close();
        Thread.sleep(50);
        pool.get();
        assertEquals(2, connectionFactory.getConnections().size());
    }

    @Test
    public void shouldExpireConnectionAfterMaxLifeTimeOnClose() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(5, TimeUnit.MILLISECONDS).build());
        Connection connection = pool.get();
        Thread.sleep(10);
        connection.close();
        assertTrue(connectionFactory.getConnections().get(0).isClosed());
    }

    @Test
    public void shouldExpireConnectionAfterMaxIdleTime() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionIdleTime(5, TimeUnit.MILLISECONDS).build());
        Connection connection = pool.get();
        connection.sendMessage(null);
        connection.close();
        Thread.sleep(50);
        pool.get();
        assertEquals(2, connectionFactory.getConnections().size());
    }

    @Test
    public void shouldCloseConnectionAfterExpiration() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(5, TimeUnit.MILLISECONDS).build());
        pool.get().close();
        Thread.sleep(50);
        pool.get();
        assertTrue(connectionFactory.getConnections().get(0).isClosed());
    }

    @Test
    public void shouldCreateNewConnectionAfterExpiration() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(5, TimeUnit.MILLISECONDS).build());
        pool.get().close();
        Thread.sleep(50);
        assertNotNull(pool.get());
        assertEquals(2, connectionFactory.getConnections().size());
    }

    @Test
    public void shouldExpireAllConnectionsAfterException() throws InterruptedException {
        pool = new DefaultConnectionProvider(getPrimary(), connectionFactory,
                DefaultConnectionProviderSettings.builder()
                        .maxSize(2).maxWaitQueueSize(1).maxConnectionLifeTime(5, TimeUnit.MILLISECONDS).build());
        Connection c1 = pool.get();
        Connection c2 = pool.get();
        connectionFactory.getConnections().get(1).throwOnSend(new MongoSocketWriteException("", getPrimary(), new IOException()));
        try {
            c2.sendMessage(Collections.<ByteBuf>emptyList());
        } catch (MongoSocketWriteException e) {
            // all good
        }
        finally {
            c1.close();
            c2.close();
        }
        pool.get();
        assertEquals(3, connectionFactory.getConnections().size());
    }


    private static class TimeoutTrackingConnectionGetter implements Runnable {
        private final ConnectionProvider connectionProvider;
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile boolean gotTimeout;

        public TimeoutTrackingConnectionGetter(final ConnectionProvider connectionProvider) {
            this.connectionProvider = connectionProvider;
        }

        @Override
        public void run() {
            try {
                connectionProvider.get();
            } catch (MongoTimeoutException e) {
                gotTimeout = true;
            } finally {
                latch.countDown();
            }
        }
    }
}

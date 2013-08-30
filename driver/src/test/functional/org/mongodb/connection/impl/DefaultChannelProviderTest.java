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
import org.junit.Test;
import org.mongodb.connection.Channel;
import org.mongodb.connection.MongoWaitQueueFullException;
import org.mongodb.connection.ServerAddress;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * These tests are racy, so doing them in Java instead of Groovy to reduce chance of failure.
 */
public class DefaultChannelProviderTest {
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress();

    private final TestConnectionFactory connectionFactory = new TestConnectionFactory();

    private DefaultChannelProvider provider;

    @After
    public void cleanup() {
        provider.close();
    }

    @Test
    public void shouldThrowOnTimeout() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS, connectionFactory,
                ChannelProviderSettings.builder()
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
        provider = new DefaultChannelProvider(SERVER_ADDRESS, connectionFactory,
                ChannelProviderSettings.builder()
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
    public void shouldExpireChannelAfterMaxLifeTime() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS, connectionFactory,
                ChannelProviderSettings.builder()
                        .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(20, MILLISECONDS).build());

        // when
        provider.get().close();
        Thread.sleep(50);
        provider.doMaintenance();
        provider.get();

        // then
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldExpireChannelAfterLifeTimeOnClose() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS,
                connectionFactory,
                ChannelProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionLifeTime(20, MILLISECONDS).build());

        // when
        Channel channel = provider.get();
        Thread.sleep(50);
        channel.close();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldExpireChanelAfterMaxIdleTime() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS,
                connectionFactory,
                ChannelProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionIdleTime(20, MILLISECONDS).build());

        // when
        Channel channel = provider.get();
        channel.close();
        Thread.sleep(50);
        provider.doMaintenance();
        provider.get();

        // then
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldCloseChannelAfterExpiration() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS,
                connectionFactory,
                ChannelProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
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
    public void shouldCreateNewChannelAfterExpiration() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS,
                connectionFactory,
                ChannelProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxConnectionLifeTime(20, MILLISECONDS).build());

        // when
        provider.get().close();
        Thread.sleep(50);
        provider.doMaintenance();
        Channel secondChannel = provider.get();

        // then
        assertNotNull(secondChannel);
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldPruneAfterMaintenanceTaskRuns() throws InterruptedException {
        // given
        provider = new DefaultChannelProvider(SERVER_ADDRESS,
                connectionFactory,
                ChannelProviderSettings.builder()
                        .maxSize(10)
                        .maxConnectionLifeTime(1, MILLISECONDS)
                        .maintenanceFrequency(5, MILLISECONDS)
                        .maxWaitQueueSize(1)
                        .build());
        provider.get().close();

        // when
        Thread.sleep(10);
        provider.doMaintenance();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }
}

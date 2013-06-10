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

import org.bson.ByteBuf;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mongodb.Fixture.getPrimary;

public class DefaultConnectionProviderTest {

    @Test
    public void shouldGetConnection() throws InterruptedException {
        ConnectionProvider connectionProvider = new DefaultConnectionProvider(getPrimary(), new TestConnectionFactory(),
                DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        Connection first = connectionProvider.get();
        assertNotNull(first);
    }

    @Test
    public void shouldThrowIfPoolIsExhausted() throws InterruptedException {
        ConnectionProvider connectionProvider = new DefaultConnectionProvider(getPrimary(), new TestConnectionFactory(),
                DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        Connection first = connectionProvider.get();
        assertNotNull(first);

        try {
            connectionProvider.get();
            fail();
        } catch (MongoTimeoutException e) {
           // all good
        }
    }

    @Test
    public void shouldThrowIfWaitQueueIsFull() throws InterruptedException {
        ConnectionProvider connectionProvider = new DefaultConnectionProvider(getPrimary(), new TestConnectionFactory(),
                DefaultConnectionProviderSettings.builder()
                        .maxSize(1)
                        .maxWaitQueueSize(1)
                        .maxWaitTime(1, TimeUnit.SECONDS)
                        .build());

        Connection first = connectionProvider.get();
        assertNotNull(first);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean gotTimeout = new AtomicBoolean(false);
        AtomicBoolean gotWaitQueueFull = new AtomicBoolean(false);

        TestConnectionGetter connectionGetter = new TestConnectionGetter(connectionProvider, gotTimeout, gotWaitQueueFull, latch);
        new Thread(connectionGetter).start();
        new Thread(connectionGetter).start();

        latch.await();

        assertTrue(gotTimeout.get());
        assertTrue(gotWaitQueueFull.get());
    }

    private static class TestConnectionGetter implements Runnable {
        private final ConnectionProvider connectionProvider;
        private final AtomicBoolean gotTimeout;
        private final AtomicBoolean gotWaitQueueFull;
        private final CountDownLatch latch;

        public TestConnectionGetter(final ConnectionProvider connectionProvider, final AtomicBoolean gotTimeout,
                                    final AtomicBoolean gotWaitQueueFull, final CountDownLatch latch) {
            this.connectionProvider = connectionProvider;
            this.gotTimeout = gotTimeout;
            this.gotWaitQueueFull = gotWaitQueueFull;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                connectionProvider.get();
            } catch (MongoTimeoutException e) {
                gotTimeout.set(true);
            } catch (MongoWaitQueueFullException e) {
                gotWaitQueueFull.set(true);
            }
            latch.countDown();
        }
    }

    private static class TestConnectionFactory implements ConnectionFactory {
        @Override
        public Connection create(final ServerAddress serverAddress) {
            return new Connection() {
                @Override
                public void sendMessage(final List<ByteBuf> byteBuffers) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ResponseBuffers receiveMessage(final ResponseSettings responseSettings) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() {
                }

                @Override
                public boolean isClosed() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ServerAddress getServerAddress() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}

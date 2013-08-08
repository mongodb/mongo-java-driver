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
import org.mongodb.connection.impl.ConnectionProviderSettings;
import org.mongodb.connection.impl.DefaultAsyncConnectionProvider;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mongodb.Fixture.getPrimary;


public class DefaultAsyncConnectionProviderTest {
    @Test
    public void shouldGetConnection() throws InterruptedException {
        AsyncConnectionProvider connectionProvider = new DefaultAsyncConnectionProvider(getPrimary(), new TestAsyncConnectionFactory(),
            ConnectionProviderSettings.builder()
                .maxSize(1)
                .maxWaitQueueSize(1)
                .build());

        AsyncConnection first = connectionProvider.get();
        assertNotNull(first);
    }

    @Test
    public void shouldThrowIfPoolIsExhausted() throws InterruptedException {
        AsyncConnectionProvider connectionProvider = new DefaultAsyncConnectionProvider(getPrimary(), new TestAsyncConnectionFactory(),
            ConnectionProviderSettings.builder()
                .maxSize(1)
                .maxWaitQueueSize(1)
                .build());

        AsyncConnection first = connectionProvider.get();
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
        AsyncConnectionProvider connectionProvider = new DefaultAsyncConnectionProvider(getPrimary(), new TestAsyncConnectionFactory(),
            ConnectionProviderSettings.builder()
                .maxSize(1)
                .maxWaitQueueSize(1)
                .maxWaitTime(1, TimeUnit.SECONDS)
                .build());


        AsyncConnection first = connectionProvider.get();
        assertNotNull(first);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean gotTimeout = new AtomicBoolean(false);
        AtomicBoolean gotWaitQueueFull = new AtomicBoolean(false);

        TestAsyncConnectionGetter connectionGetter = new TestAsyncConnectionGetter(connectionProvider, gotTimeout, gotWaitQueueFull, latch);
        new Thread(connectionGetter).start();
        new Thread(connectionGetter).start();

        latch.await();

        assertTrue(gotTimeout.get());
        assertTrue(gotWaitQueueFull.get());
    }

    private static class TestAsyncConnectionGetter implements Runnable {
        private final AsyncConnectionProvider connectionProvider;
        private final AtomicBoolean gotTimeout;
        private final AtomicBoolean gotWaitQueueFull;
        private final CountDownLatch latch;

        public TestAsyncConnectionGetter(final AsyncConnectionProvider connectionProvider, final AtomicBoolean gotTimeout,
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

    private static class TestAsyncConnectionFactory implements AsyncConnectionFactory {
        @Override
        public AsyncConnection create(final ServerAddress serverAddress) {
            return new AsyncConnection() {
                @Override
                public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
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

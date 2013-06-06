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

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
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
        final ConnectionProvider connectionProvider = new DefaultConnectionProvider(getPrimary(), new TestConnectionFactory(),
                DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1)
                        .maxWaitTime(Long.MAX_VALUE, TimeUnit.MILLISECONDS).build());

        Connection first = connectionProvider.get();
        assertNotNull(first);

        final CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
                connectionProvider.get();
            }
        });

        t.start();

        latch.await();

        // this test is racy, but not sure how to make it otherwise.
        try {
            connectionProvider.get();
            fail();
        } catch (MongoWaitQueueFullException e) {
            // all good
        }
    }

    private class TestConnectionFactory implements ConnectionFactory {
        @Override
        public Connection create(final ServerAddress serverAddress) {
            return new Connection() {
                @Override
                public void sendMessage(final ChannelAwareOutputBuffer buffer) {
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

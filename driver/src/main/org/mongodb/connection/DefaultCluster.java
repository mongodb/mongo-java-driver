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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoInterruptedException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public abstract class DefaultCluster implements Cluster {

    private final AtomicReference<CountDownLatch> phase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final BufferPool<ByteBuffer> bufferPool;
    private final List<MongoCredential> credentialList;
    private final MongoClientOptions options;
    private final ServerFactory serverFactory;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ThreadLocal<Random> random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    public DefaultCluster(final BufferPool<ByteBuffer> bufferPool, final List<MongoCredential> credentialList,
                          final MongoClientOptions options, final ServerFactory serverFactory) {
        this.credentialList = credentialList;
        this.options = notNull("options", options);
        this.serverFactory = notNull("serverFactory", serverFactory);
        this.bufferPool = notNull("bufferPool", bufferPool);
        scheduledExecutorService = Executors.newScheduledThreadPool(3);  // TODO: configurable
    }

    @Override
    public Server getServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            List<ServerDescription> serverDescriptions = serverSelector.choose(description);
            long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS); // TODO: configurable
            while (serverDescriptions.isEmpty()) {
                if (!currentPhase.await(endTime - System.nanoTime(), TimeUnit.NANOSECONDS)) {
                    throw new MongoTimeoutException(
                            "Thread timed out while waiting for a server that satisfied server selector: " + serverSelector);
                }
                serverDescriptions = serverSelector.choose(description);
                currentPhase = phase.get();
            }
            return getServer(getRandomServer(serverDescriptions).getAddress());
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(
                    "Thread was interrupted while waiting for a server that satisfied server selector: " + serverSelector, e);
        }
    }

    @Override
    public ClusterDescription getDescription() {
        return description;
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());

        return bufferPool;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            scheduledExecutorService.shutdownNow();
            phase.get().countDown();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected abstract Server getServer(final ServerAddress serverAddress);

    protected synchronized void updateDescription(final ClusterDescription newDescription) {
        description = newDescription;
        CountDownLatch current = phase.getAndSet(new CountDownLatch(1));
        current.countDown();
    }

    private ServerDescription getRandomServer(final List<ServerDescription> serverDescriptions) {
        return serverDescriptions.get(getRandom().nextInt(serverDescriptions.size()));
    }

    protected Random getRandom() {
        return random.get();
    }

    protected Server createServer(final ServerAddress serverAddress, final ChangeListener<ServerDescription> serverStateListener) {
        final Server server = serverFactory.create(serverAddress, credentialList, options, scheduledExecutorService, bufferPool);
        server.addChangeListener(serverStateListener);
        return server;
    }
}

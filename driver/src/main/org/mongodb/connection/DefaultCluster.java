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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;

public abstract class DefaultCluster implements Cluster {

    private final Phaser clusterPhaser = new Phaser(1);
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
    private volatile ClusterDescription description = new ClusterDescription(Collections.<ServerDescription>emptyList(),
            SLAVE_ACCEPTABLE_LATENCY_MS);

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
            int phaseNumber = clusterPhaser.getPhase();
            List<ServerDescription> serverDescriptions = serverSelector.choose(description);
            long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS); // TODO: configurable
            while (serverDescriptions.isEmpty()) {
                clusterPhaser.awaitAdvanceInterruptibly(phaseNumber, endTime - System.nanoTime(), TimeUnit.NANOSECONDS);
                serverDescriptions = serverSelector.choose(description);
                phaseNumber = clusterPhaser.getPhase();
            }
            return getServer(getRandomServer(serverDescriptions).getAddress());
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(
                    "Thread was interrupted while waiting for a server that satisfied server selector: " + serverSelector, e);
        } catch (TimeoutException e) {
            throw new MongoTimeoutException(
                    "Thread timed out while waiting for a server that satisfied server selector: " + serverSelector);
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
            clusterPhaser.forceTermination();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected synchronized void updateDescription(final ClusterDescription newDescription) {
        description = newDescription;
        clusterPhaser.arrive();
    }

    private ServerDescription getRandomServer(final List<ServerDescription> serverDescriptions) {
        return serverDescriptions.get(getRandom().nextInt(serverDescriptions.size()));
    }

    protected Random getRandom() {
        return random.get();
    }

    protected Server createServer(final ServerAddress serverAddress, final ServerStateListener serverStateListener) {
        final Server server = serverFactory.create(serverAddress, credentialList, options, scheduledExecutorService, bufferPool);
        server.addChangeListener(serverStateListener);
        return server;
    }
}

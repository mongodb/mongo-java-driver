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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.bson.util.Assertions.isTrue;
import static org.bson.util.Assertions.notNull;

abstract class BaseCluster implements Cluster {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final AtomicReference<CountDownLatch> phase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final ClusterableServerFactory serverFactory;
    private final ThreadLocal<Random> random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };
    private final String clusterId;
    private final ClusterSettings settings;
    private final ClusterListener clusterListener;

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    public BaseCluster(final String clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                       final ClusterListener clusterListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        this.serverFactory = notNull("serverFactory", serverFactory);
        this.clusterListener = notNull("clusterListener", clusterListener);
        clusterListener.clusterOpened(new ClusterEvent(clusterId));
    }

    @Override
    public Server getServer(final ServerSelector serverSelector, final long maxWaitTime, final TimeUnit timeUnit) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            List<ServerDescription> serverDescriptions = serverSelector.choose(curDescription);
            final long endTime = System.nanoTime() + NANOSECONDS.convert(maxWaitTime, timeUnit);
            while (true) {
                throwIfIncompatible(curDescription);
                if (!serverDescriptions.isEmpty()) {
                    ClusterableServer server = getRandomServer(new ArrayList<ServerDescription>(serverDescriptions));
                    if (server != null) {
                        return new WrappedServer(server);
                    }
                }

                if (!curDescription.isConnecting()) {
                    throw new MongoServerSelectionException(format("Unable to connect to any server that matches %s", serverSelector));
                }

                final long timeout = endTime - System.nanoTime();

                LOGGER.info(format("No server chosen by %s from cluster description %s. Waiting for %d ms before timing out",
                                   serverSelector, curDescription, MILLISECONDS.convert(timeout, NANOSECONDS)));

                if (!currentPhase.await(timeout, NANOSECONDS)) {
                    throw new MongoTimeoutException(format("Timed out while waiting for a server that matches %s after %d ms",
                                                           serverSelector, MILLISECONDS.convert(timeout, NANOSECONDS)));
                }
                currentPhase = phase.get();
                curDescription = description;
                serverDescriptions = serverSelector.choose(curDescription);
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(format("Interrupted while waiting for a server that matches %s ", serverSelector), e);
        }
    }

    @Override
    public ClusterDescription getDescription(final long maxWaitTime, final TimeUnit timeUnit) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            final long endTime = System.nanoTime() + NANOSECONDS.convert(maxWaitTime, timeUnit);
            while (curDescription.getType() == ClusterType.Unknown) {

                if (!curDescription.isConnecting()) {
                    throw new MongoServerSelectionException(format("Unable to connect to any servers"));
                }

                final long timeout = endTime - System.nanoTime();

                LOGGER.info(format("Cluster description not yet available. Waiting for %d ms before timing out",
                                   MILLISECONDS.convert(timeout, NANOSECONDS)));

                if (!currentPhase.await(timeout, NANOSECONDS)) {
                    throw new MongoTimeoutException(format("Timed out while waiting to connect after %d ms",
                                                           MILLISECONDS.convert(timeout, NANOSECONDS)));
                }
                currentPhase = phase.get();
                curDescription = description;
            }
            return curDescription;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(format("Interrupted while waiting to connect"), e);
        }
    }

    public ClusterSettings getSettings() {
        return settings;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            serverFactory.close();
            phase.get().countDown();
            clusterListener.clusterClosed(new ClusterEvent(clusterId));
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Return the server at the given address.
     *
     * @param serverAddress the address
     * @return the server, or null if the cluster no longer contains a server at this address.
     */
    protected abstract ClusterableServer getServer(final ServerAddress serverAddress);

    protected synchronized void updateDescription(final ClusterDescription newDescription) {
        LOGGER.fine(format("Updating cluster description to  %s", newDescription.getShortDescription()));

        description = newDescription;
        final CountDownLatch current = phase.getAndSet(new CountDownLatch(1));
        current.countDown();
    }

    protected void fireChangeEvent() {
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, description));
    }

    // gets a random server that still exists in the cluster.  Returns null if there are none.
    private ClusterableServer getRandomServer(final List<ServerDescription> serverDescriptions) {
        while (!serverDescriptions.isEmpty()) {
            int serverPos = getRandom().nextInt(serverDescriptions.size());
            ClusterableServer server = getServer(serverDescriptions.get(serverPos).getAddress());
            if (server != null) {
                return server;
            }
            else {
                serverDescriptions.remove(serverPos);
            }
        }
        return null;
    }

    private void throwIfIncompatible(final ClusterDescription curDescription) {
        if (!curDescription.isCompatibleWithDriver()) {
            throw new MongoIncompatibleDriverException(format("This version of the driver is not compatible with one or more of the " +
                                                              "servers to which it is connected: %s", curDescription));
        }
    }

    protected Random getRandom() {
        return random.get();
    }

    protected ClusterableServer createServer(final ServerAddress serverAddress, final ChangeListener<ServerDescription>
                                                                                serverStateListener) {
        final ClusterableServer server = serverFactory.create(serverAddress);
        server.addChangeListener(serverStateListener);
        return server;
    }

    private static final class WrappedServer implements Server {
        private final ClusterableServer wrapped;

        public WrappedServer(final ClusterableServer server) {
            wrapped = server;
        }

        @Override
        public ServerDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public Connection getConnection(final long maxWaitTime, final TimeUnit timeUnit) {
            return wrapped.getConnection(maxWaitTime, timeUnit);
        }

        @Override
        public void invalidate() {
            wrapped.invalidate();
        }
    }
}

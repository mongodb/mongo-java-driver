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

package com.mongodb.connection;

import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

abstract class BaseCluster implements Cluster {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final AtomicReference<CountDownLatch> phase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final ClusterableServerFactory serverFactory;
    private final ThreadLocal<Random> random = new ThreadLocal<Random>();
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
    public Server selectServer(final ServerSelector serverSelector, final long maxWaitTime, final TimeUnit timeUnit) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            ServerSelector compositeServerSelector = getCompositeServerSelector(serverSelector);
            List<ServerDescription> serverDescriptions = compositeServerSelector.select(curDescription);

            boolean selectionFailureLogged = false;

            long startTimeNanos = System.nanoTime();
            long endTimeNanos = startTimeNanos + NANOSECONDS.convert(maxWaitTime, timeUnit);
            long curTimeNanos = startTimeNanos;

            while (true) {
                throwIfIncompatible(curDescription);

                if (!serverDescriptions.isEmpty()) {
                    ClusterableServer server = getRandomServer(new ArrayList<ServerDescription>(serverDescriptions));
                    if (server != null) {
                        return server;
                    }
                }

                if (curTimeNanos > endTimeNanos) {
                    throw new MongoTimeoutException(format("Timed out while waiting for a server that matches %s after %d ms",
                                                           serverSelector, MILLISECONDS.convert(maxWaitTime, timeUnit)));
                }

                if (!selectionFailureLogged) {
                    LOGGER.info(format("No server chosen by %s from cluster description %s. Waiting for %d ms before timing out",
                                       serverSelector, curDescription, MILLISECONDS.convert(maxWaitTime, timeUnit)));
                    selectionFailureLogged = true;
                }

                connect();

                currentPhase.await(Math.min(endTimeNanos - curTimeNanos,
                                            serverFactory.getSettings().getMinHeartbeatFrequency(NANOSECONDS)),
                                   NANOSECONDS);

                curTimeNanos = System.nanoTime();

                currentPhase = phase.get();
                curDescription = description;
                serverDescriptions = compositeServerSelector.select(curDescription);
            }

        } catch (InterruptedException e) {
            throw new MongoInterruptedException(format("Interrupted while waiting for a server that matches %s", serverSelector), e);
        }
    }

    private ServerSelector getCompositeServerSelector(final ServerSelector serverSelector) {
        if (settings.getServerSelector() == null) {
            return serverSelector;
        } else {
            return new CompositeServerSelector(asList(serverSelector, settings.getServerSelector()));
        }
    }

    @Override
    public ClusterDescription getDescription(final long maxWaitTime, final TimeUnit timeUnit) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;

            boolean selectionFailureLogged = false;

            long startTimeNanos = System.nanoTime();
            long endTimeNanos = startTimeNanos + NANOSECONDS.convert(maxWaitTime, timeUnit);
            long curTimeNanos = startTimeNanos;

            while (curDescription.getType() == ClusterType.UNKNOWN) {

                if (curTimeNanos > endTimeNanos) {
                    throw new com.mongodb.MongoTimeoutException(format("Timed out while waiting to connect after %d ms",
                                                           MILLISECONDS.convert(maxWaitTime, timeUnit)));
                }

                if (!selectionFailureLogged) {
                    LOGGER.info(format("Cluster description not yet available. Waiting for %d ms before timing out",
                                       MILLISECONDS.convert(maxWaitTime, timeUnit)));
                    selectionFailureLogged = true;
                }

                connect();

                currentPhase.await(Math.min(endTimeNanos - curTimeNanos,
                                            serverFactory.getSettings().getMinHeartbeatFrequency(NANOSECONDS)),
                                   NANOSECONDS);

                curTimeNanos = System.nanoTime();

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

    protected abstract void connect();

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
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
    protected abstract ClusterableServer getServer(ServerAddress serverAddress);

    protected synchronized void updateDescription(final ClusterDescription newDescription) {
        LOGGER.debug(format("Updating cluster description to  %s", newDescription.getShortDescription()));

        description = newDescription;
        CountDownLatch current = phase.getAndSet(new CountDownLatch(1));
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
            } else {
                serverDescriptions.remove(serverPos);
            }
        }
        return null;
    }

    // it's important that Random instances are created in this way instead of via subclassing ThreadLocal and overriding the
    // initialValue() method.
    private Random getRandom() {
        Random result = random.get();
        if (result == null) {
            result = new Random();
            random.set(result);
        }
        return result;
    }

    protected ClusterableServer createServer(final ServerAddress serverAddress,
                                             final ChangeListener<ServerDescription> serverStateListener) {
        ClusterableServer server = serverFactory.create(serverAddress);
        server.addChangeListener(serverStateListener);
        return server;
    }

    private void throwIfIncompatible(final ClusterDescription curDescription) {
        if (!curDescription.isCompatibleWithDriver()) {
            throw new MongoIncompatibleDriverException(format("This version of the driver is not compatible with one or more of "
                                                              + "the servers to which it is connected: %s", curDescription),
                                                       curDescription);
        }
    }
}

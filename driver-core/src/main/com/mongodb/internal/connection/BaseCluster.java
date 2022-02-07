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

package com.mongodb.internal.connection;

import com.mongodb.MongoClientException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonTimestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_SERVER_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.connection.EventHelper.wouldDescriptionsGenerateEquivalentEvents;
import static com.mongodb.internal.event.EventListenerHelper.createServerListener;
import static com.mongodb.internal.event.EventListenerHelper.getClusterListener;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

abstract class BaseCluster implements Cluster {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final AtomicReference<CountDownLatch> phase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final ClusterableServerFactory serverFactory;
    private final ClusterId clusterId;
    private final ClusterSettings settings;
    private final ClusterListener clusterListener;
    private final Deque<ServerSelectionRequest> waitQueue = new ConcurrentLinkedDeque<ServerSelectionRequest>();
    private final ClusterClock clusterClock = new ClusterClock();
    private Thread waitQueueHandler;

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    BaseCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        this.serverFactory = notNull("serverFactory", serverFactory);
        this.clusterListener = getClusterListener(settings);
        clusterListener.clusterOpening(new ClusterOpeningEvent(clusterId));
        description = new ClusterDescription(settings.getMode(), ClusterType.UNKNOWN, Collections.<ServerDescription>emptyList(),
                settings, serverFactory.getSettings());
    }

    @Override
    public BsonTimestamp getClusterTime() {
        return clusterClock.getClusterTime();
    }

    @Override
    public ServerTuple selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            ServerSelector compositeServerSelector = getCompositeServerSelector(serverSelector);
            ServerTuple serverTuple = selectServer(compositeServerSelector, curDescription);

            boolean selectionFailureLogged = false;

            long startTimeNanos = System.nanoTime();
            long curTimeNanos = startTimeNanos;
            long maxWaitTimeNanos = getMaxWaitTimeNanos();

            while (true) {
                throwIfIncompatible(curDescription);

                if (serverTuple != null) {
                    return serverTuple;
                }

                if (curTimeNanos - startTimeNanos > maxWaitTimeNanos) {
                    throw createTimeoutException(serverSelector, curDescription);
                }

                if (!selectionFailureLogged) {
                    logServerSelectionFailure(serverSelector, curDescription);
                    selectionFailureLogged = true;
                }

                connect();

                currentPhase.await(Math.min(maxWaitTimeNanos - (curTimeNanos - startTimeNanos), getMinWaitTimeNanos()), NANOSECONDS);

                curTimeNanos = System.nanoTime();

                currentPhase = phase.get();
                curDescription = description;
                serverTuple = selectServer(compositeServerSelector, curDescription);
            }

        } catch (InterruptedException e) {
            throw new MongoInterruptedException(format("Interrupted while waiting for a server that matches %s", serverSelector), e);
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<ServerTuple> callback) {
        isTrue("open", !isClosed());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Asynchronously selecting server with selector %s", serverSelector));
        }
        ServerSelectionRequest request = new ServerSelectionRequest(serverSelector, getCompositeServerSelector(serverSelector),
                                                                    getMaxWaitTimeNanos(), callback);

        CountDownLatch currentPhase = phase.get();
        ClusterDescription currentDescription = description;

        if (!handleServerSelectionRequest(request, currentPhase, currentDescription)) {
            notifyWaitQueueHandler(request);
        }
    }

    @Override
    public ClusterDescription getDescription() {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;

            boolean selectionFailureLogged = false;

            long startTimeNanos = System.nanoTime();
            long curTimeNanos = startTimeNanos;
            long maxWaitTimeNanos = getMaxWaitTimeNanos();

            while (curDescription.getType() == ClusterType.UNKNOWN) {

                if (curTimeNanos - startTimeNanos > maxWaitTimeNanos) {
                    throw new MongoTimeoutException(format("Timed out after %d ms while waiting to connect. Client view of cluster state "
                                                           + "is %s",
                                                           settings.getServerSelectionTimeout(MILLISECONDS),
                                                           curDescription.getShortDescription()));
                }

                if (!selectionFailureLogged) {
                    if (LOGGER.isInfoEnabled()) {
                        if (settings.getServerSelectionTimeout(MILLISECONDS) < 0) {
                            LOGGER.info("Cluster description not yet available. Waiting indefinitely.");
                        } else {
                            LOGGER.info(format("Cluster description not yet available. Waiting for %d ms before timing out",
                                               settings.getServerSelectionTimeout(MILLISECONDS)));
                        }
                    }
                    selectionFailureLogged = true;
                }

                connect();

                currentPhase.await(Math.min(maxWaitTimeNanos - (curTimeNanos - startTimeNanos), getMinWaitTimeNanos()), NANOSECONDS);

                curTimeNanos = System.nanoTime();

                currentPhase = phase.get();
                curDescription = description;
            }
            return curDescription;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted while waiting to connect", e);
        }
    }

    public ClusterId getClusterId() {
        return clusterId;
    }

    public ClusterSettings getSettings() {
        return settings;
    }

    public ClusterableServerFactory getServerFactory() {
        return serverFactory;
    }

    protected abstract void connect();

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            phase.get().countDown();
            clusterListener.clusterClosed(new ClusterClosedEvent(clusterId));
            stopWaitQueueHandler();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected void updateDescription(final ClusterDescription newDescription) {
        withLock(() -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Updating cluster description to  %s", newDescription.getShortDescription()));
            }

            description = newDescription;
            updatePhase();
        });
    }

    protected void fireChangeEvent(final ClusterDescription newDescription, final ClusterDescription previousDescription) {
        if (!wouldDescriptionsGenerateEquivalentEvents(newDescription, previousDescription)) {
             clusterListener.clusterDescriptionChanged(
                     new ClusterDescriptionChangedEvent(getClusterId(), newDescription, previousDescription));
        }
    }

    @Override
    public ClusterDescription getCurrentDescription() {
        return description;
    }

    @Override
    public synchronized void withLock(final Runnable action) {
        action.run();
    }

    private void updatePhase() {
        withLock(() -> phase.getAndSet(new CountDownLatch(1)).countDown());
    }

    private long getMaxWaitTimeNanos() {
        if (settings.getServerSelectionTimeout(NANOSECONDS) < 0) {
            return Long.MAX_VALUE;
        }
        return settings.getServerSelectionTimeout(NANOSECONDS);
    }

    private long getMinWaitTimeNanos() {
        return serverFactory.getSettings().getMinHeartbeatFrequency(NANOSECONDS);
    }

    private boolean handleServerSelectionRequest(final ServerSelectionRequest request, final CountDownLatch currentPhase,
                                                 final ClusterDescription description) {
        try {
            if (currentPhase != request.phase) {
                CountDownLatch prevPhase = request.phase;
                request.phase = currentPhase;
                if (!description.isCompatibleWithDriver()) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Asynchronously failed server selection due to driver incompatibility with server");
                    }
                    request.onResult(null, createIncompatibleException(description));
                    return true;
                }

                ServerTuple serverTuple = selectServer(request.compositeSelector, description);
                if (serverTuple != null) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("Asynchronously selected server %s", serverTuple.getServerDescription().getAddress()));
                    }
                    request.onResult(serverTuple, null);
                    return true;
                }
                if (prevPhase == null) {
                    logServerSelectionFailure(request.originalSelector, description);
                }
            }

            if (request.timedOut()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Asynchronously failed server selection after timeout");
                }
                request.onResult(null, createTimeoutException(request.originalSelector, description));
                return true;
            }

            return false;
        } catch (Exception e) {
            request.onResult(null, e);
            return true;
        }
    }

    private void logServerSelectionFailure(final ServerSelector serverSelector, final ClusterDescription curDescription) {
        if (LOGGER.isInfoEnabled()) {
            if (settings.getServerSelectionTimeout(MILLISECONDS) < 0) {
                LOGGER.info(format("No server chosen by %s from cluster description %s. Waiting indefinitely.",
                                   serverSelector, curDescription));
            } else {
                LOGGER.info(format("No server chosen by %s from cluster description %s. Waiting for %d ms before timing out",
                                   serverSelector, curDescription, settings.getServerSelectionTimeout(MILLISECONDS)));
            }
        }
    }

    @Nullable
    private ServerTuple selectServer(final ServerSelector serverSelector, final ClusterDescription clusterDescription) {
        return selectServer(serverSelector, clusterDescription, this::getServer);
    }

    @Nullable
    @VisibleForTesting(otherwise = PRIVATE)
    static ServerTuple selectServer(final ServerSelector serverSelector, final ClusterDescription clusterDescription,
            final Function<ServerAddress, Server> serverCatalog) {
        return atMostNRandom(new ArrayList<>(serverSelector.select(clusterDescription)), 2, serverDescription -> {
            Server server = serverCatalog.apply(serverDescription.getAddress());
            return server == null ? null : new ServerTuple(server, serverDescription);
        }).stream()
                .min(comparingInt(serverTuple -> serverTuple.getServer().operationCount()))
                .orElse(null);
    }

    /**
     * Returns a new {@link List} of at most {@code n} elements, where each element is a result of
     * {@linkplain Function#apply(Object) applying} the {@code transformer} to a randomly picked element from the specified {@code list},
     * such that no element is picked more than once. If the {@code transformer} produces {@code null}, then another element is picked
     * until either {@code n} transformed non-{@code null} elements are collected, or the {@code list} does not have
     * unpicked elements left.
     * <p>
     * Note that this method may reorder the {@code list}, as it uses the
     * <a href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle">Fisher–Yates, a.k.a. Durstenfeld, shuffle algorithm</a>.
     */
    private static List<ServerTuple> atMostNRandom(final ArrayList<ServerDescription> list, final int n,
            final Function<ServerDescription, ServerTuple> transformer) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<ServerTuple> result = new ArrayList<>(n);
        for (int i = list.size() - 1; i >= 0 && result.size() < n; i--) {
            Collections.swap(list, i, random.nextInt(i + 1));
            ServerTuple serverTuple = transformer.apply(list.get(i));
            if (serverTuple != null) {
                result.add(serverTuple);
            }
        }
        return result;
    }

    private ServerSelector getCompositeServerSelector(final ServerSelector serverSelector) {
        if (settings.getServerSelector() == null) {
            return serverSelector;
        } else {
            return new CompositeServerSelector(asList(serverSelector, settings.getServerSelector()));
        }
    }

    protected ClusterableServer createServer(final ServerAddress serverAddress,
                                             final ServerDescriptionChangedListener serverDescriptionChangedListener) {
        return serverFactory.create(this, serverAddress, serverDescriptionChangedListener,
                createServerListener(serverFactory.getSettings()), clusterClock);
    }

    private void throwIfIncompatible(final ClusterDescription curDescription) {
        if (!curDescription.isCompatibleWithDriver()) {
            throw createIncompatibleException(curDescription);
        }
    }

    private MongoIncompatibleDriverException createIncompatibleException(final ClusterDescription curDescription) {
        String message;
        ServerDescription incompatibleServer = curDescription.findServerIncompatiblyOlderThanDriver();
        if (incompatibleServer != null) {
            message = format("Server at %s reports wire version %d, but this version of the driver requires at least %d (MongoDB %s).",
                    incompatibleServer.getAddress(), incompatibleServer.getMaxWireVersion(),
                    MIN_DRIVER_WIRE_VERSION, MIN_DRIVER_SERVER_VERSION);
        } else {
            incompatibleServer = curDescription.findServerIncompatiblyNewerThanDriver();
            if (incompatibleServer != null) {
                message = format("Server at %s requires wire version %d, but this version of the driver only supports up to %d.",
                        incompatibleServer.getAddress(), incompatibleServer.getMinWireVersion(), MAX_DRIVER_WIRE_VERSION);
            } else {
                throw new IllegalStateException("Server can't be both older than the driver and newer.");
            }
        }
        return new MongoIncompatibleDriverException(message, curDescription);
    }

    private MongoTimeoutException createTimeoutException(final ServerSelector serverSelector, final ClusterDescription curDescription) {
        return new MongoTimeoutException(format("Timed out after %d ms while waiting for a server that matches %s. "
                                                + "Client view of cluster state is %s",
                                                settings.getServerSelectionTimeout(MILLISECONDS), serverSelector,
                                                curDescription.getShortDescription()));
    }

    private static final class ServerSelectionRequest {
        private final ServerSelector originalSelector;
        private final ServerSelector compositeSelector;
        private final long maxWaitTimeNanos;
        private final SingleResultCallback<ServerTuple> callback;
        private final long startTimeNanos = System.nanoTime();
        private CountDownLatch phase;

        ServerSelectionRequest(final ServerSelector serverSelector, final ServerSelector compositeSelector,
                               final long maxWaitTimeNanos,
                               final SingleResultCallback<ServerTuple> callback) {
            this.originalSelector = serverSelector;
            this.compositeSelector = compositeSelector;
            this.maxWaitTimeNanos = maxWaitTimeNanos;
            this.callback = callback;
        }

        void onResult(final ServerTuple serverTuple, final Throwable t) {
            try {
                callback.onResult(serverTuple, t);
            } catch (Throwable tr) {
                // ignore
            }
        }

        boolean timedOut() {
            return System.nanoTime() - startTimeNanos > maxWaitTimeNanos;
        }

        long getRemainingTime() {
            return startTimeNanos + maxWaitTimeNanos - System.nanoTime();
        }
    }

    private void notifyWaitQueueHandler(final ServerSelectionRequest request) {
        withLock(() -> {
            if (isClosed) {
                return;
            }

            waitQueue.add(request);

            if (waitQueueHandler == null) {
                waitQueueHandler = new Thread(new WaitQueueHandler(), "cluster-" + clusterId.getValue());
                waitQueueHandler.setDaemon(true);
                waitQueueHandler.start();
            } else {
                updatePhase();
            }
        });
    }

    private void stopWaitQueueHandler() {
        withLock(() -> {
            if (waitQueueHandler != null) {
                waitQueueHandler.interrupt();
            }
        });
    }

    private final class WaitQueueHandler implements Runnable {
        public void run() {
            while (!isClosed) {
                CountDownLatch currentPhase = phase.get();
                ClusterDescription curDescription = description;
                long waitTimeNanos = Long.MAX_VALUE;

                for (Iterator<ServerSelectionRequest> iter = waitQueue.iterator(); iter.hasNext();) {
                    ServerSelectionRequest nextRequest = iter.next();
                    if (handleServerSelectionRequest(nextRequest, currentPhase, curDescription)) {
                        iter.remove();
                    } else {
                        waitTimeNanos = Math.min(nextRequest.getRemainingTime(), Math.min(getMinWaitTimeNanos(), waitTimeNanos));
                    }
                }

                // if there are any waiters that were not satisfied, connect
                if (waitTimeNanos < Long.MAX_VALUE) {
                    connect();
                }

                try {
                    currentPhase.await(waitTimeNanos, NANOSECONDS);
                } catch (InterruptedException e) {
                    // The cluster has been closed and the while loop will exit.
                }
            }
            // Notify all remaining waiters that a shutdown is in progress
            for (Iterator<ServerSelectionRequest> iter = waitQueue.iterator(); iter.hasNext();) {
                iter.next().onResult(null, new MongoClientException("Shutdown in progress"));
                iter.remove();
            }
        }
    }
}

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
import com.mongodb.MongoException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.logging.LogMessage.Entry;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.selector.LatencyMinimizingServerSelector;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_SERVER_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.internal.Locks.withInterruptibleLock;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.connection.EventHelper.wouldDescriptionsGenerateEquivalentEvents;
import static com.mongodb.internal.event.EventListenerHelper.singleClusterListener;
import static com.mongodb.internal.logging.LogMessage.Component.SERVER_SELECTION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.FAILURE;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.OPERATION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.OPERATION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REMAINING_TIME_MS;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SELECTOR;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_HOST;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_PORT;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.TOPOLOGY_DESCRIPTION;
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;
import static com.mongodb.internal.logging.LogMessage.Level.INFO;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

abstract class BaseCluster implements Cluster {

    private static final Logger LOGGER = Loggers.getLogger("cluster");
    private static final StructuredLogger STRUCTURED_LOGGER = new StructuredLogger("cluster");

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicReference<CountDownLatch> phase = new AtomicReference<>(new CountDownLatch(1));
    private final ClusterableServerFactory serverFactory;
    private final ClusterId clusterId;
    private final ClusterSettings settings;
    private final ClusterListener clusterListener;
    private final Deque<ServerSelectionRequest> waitQueue = new ConcurrentLinkedDeque<>();
    private final ClusterClock clusterClock = new ClusterClock();
    private Thread waitQueueHandler;

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    BaseCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        this.serverFactory = notNull("serverFactory", serverFactory);
        this.clusterListener = singleClusterListener(settings);
        clusterListener.clusterOpening(new ClusterOpeningEvent(clusterId));
        description = new ClusterDescription(settings.getMode(), ClusterType.UNKNOWN, Collections.emptyList(),
                settings, serverFactory.getSettings());
    }

    @Override
    public ClusterClock getClock() {
        return clusterClock;
    }

    @Override
    public ServerTuple selectServer(final ServerSelector serverSelector, final OperationContext operationContext) {
        isTrue("open", !isClosed());

        ServerSelector compositeServerSelector = getCompositeServerSelector(serverSelector);
        boolean selectionWaitingLogged = false;
        Timeout computedServerSelectionTimeout = operationContext.getTimeoutContext().computeServerSelectionTimeout();
        logServerSelectionStarted(clusterId, operationContext.getId(), serverSelector, description);

        while (true) {
            CountDownLatch currentPhaseLatch = phase.get();
            ClusterDescription currentDescription = description;
            ServerTuple serverTuple = selectServer(compositeServerSelector, currentDescription, computedServerSelectionTimeout);

            if (!currentDescription.isCompatibleWithDriver()) {
                logAndThrowIncompatibleException(operationContext.getId(), serverSelector, currentDescription);
            }
            if (serverTuple != null) {
                logServerSelectionSucceeded(clusterId, operationContext.getId(), serverTuple.getServerDescription().getAddress(),
                        serverSelector, currentDescription);
                return serverTuple;
            }
            computedServerSelectionTimeout.ifExpired(() -> {
                logAndThrowTimeoutException(operationContext.getId(), serverSelector, currentDescription);
            });
            if (!selectionWaitingLogged) {
                logServerSelectionWaiting(clusterId, operationContext.getId(), computedServerSelectionTimeout, serverSelector, currentDescription);
                selectionWaitingLogged = true;
            }
            connect();
            Timeout heartbeatLimitedTimeout = computedServerSelectionTimeout.orEarlier(startMinWaitHeartbeatTimeout());
            heartbeatLimitedTimeout.awaitOn(currentPhaseLatch,
                    () -> format("waiting for a server that matches %s", serverSelector));
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final OperationContext operationContext,
            final SingleResultCallback<ServerTuple> callback) {
        isTrue("open", !isClosed());
        Timeout computedServerSelectionTimeout = operationContext.getTimeoutContext().computeServerSelectionTimeout();
        ServerSelectionRequest request = new ServerSelectionRequest(
                serverSelector, getCompositeServerSelector(serverSelector), operationContext.getId(), computedServerSelectionTimeout,
                callback);

        CountDownLatch currentPhase = phase.get();
        ClusterDescription currentDescription = description;

        logServerSelectionStarted(clusterId, operationContext.getId(), serverSelector, currentDescription);
        if (!handleServerSelectionRequest(request, currentPhase, currentDescription)) {
            notifyWaitQueueHandler(request);
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

    /**
     * Subclasses must ensure that this method is called in a way that events are delivered in a predictable order.
     * Typically, this means calling it while holding a lock that includes both updates to the cluster state and firing the event.
     */
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
    public void withLock(final Runnable action) {
        withInterruptibleLock(lock, action);
    }

    private void updatePhase() {
        withLock(() -> phase.getAndSet(new CountDownLatch(1)).countDown());
    }

    private Timeout startMinWaitHeartbeatTimeout() {
        long minHeartbeatFrequency = serverFactory.getSettings().getMinHeartbeatFrequency(NANOSECONDS);
        minHeartbeatFrequency = Math.max(0, minHeartbeatFrequency);
        return Timeout.expiresIn(minHeartbeatFrequency, NANOSECONDS);
    }

    private boolean handleServerSelectionRequest(
            final ServerSelectionRequest request, final CountDownLatch currentPhase,
            final ClusterDescription description) {
        try {
            if (currentPhase != request.phase) {
                CountDownLatch prevPhase = request.phase;
                request.phase = currentPhase;
                if (!description.isCompatibleWithDriver()) {
                    logAndThrowIncompatibleException(request.getOperationId(), request.originalSelector, description);
                }

                ServerTuple serverTuple = selectServer(request.compositeSelector, description, request.getTimeout());
                if (serverTuple != null) {
                    logServerSelectionSucceeded(clusterId, request.getOperationId(), serverTuple.getServerDescription().getAddress(),
                            request.originalSelector, description);
                    request.onResult(serverTuple, null);
                    return true;
                }
                if (prevPhase == null) {
                    logServerSelectionWaiting(clusterId, request.getOperationId(), request.getTimeout(), request.originalSelector,
                            description);
                }
            }

            Timeout.ifExistsAndExpired(request.getTimeout(), () -> {
                logAndThrowTimeoutException(request.getOperationId(), request.originalSelector, description);
            });
            return false;
        } catch (Exception e) {
            request.onResult(null, e);
            return true;
        }
    }

    @Nullable
    private ServerTuple selectServer(final ServerSelector serverSelector,
            final ClusterDescription clusterDescription, final Timeout serverSelectionTimeout) {
        return selectServer(
                serverSelector,
                clusterDescription,
                serverAddress -> getServer(serverAddress, serverSelectionTimeout));
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
        ServerSelector latencyMinimizingServerSelector =
                new LatencyMinimizingServerSelector(settings.getLocalThreshold(MILLISECONDS), MILLISECONDS);
        if (settings.getServerSelector() == null) {
            return new CompositeServerSelector(asList(serverSelector, latencyMinimizingServerSelector));
        } else {
            return new CompositeServerSelector(asList(serverSelector, settings.getServerSelector(), latencyMinimizingServerSelector));
        }
    }

    protected ClusterableServer createServer(final ServerAddress serverAddress) {
        return serverFactory.create(this, serverAddress);
    }

    private void logAndThrowIncompatibleException(
            final long operationId,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        MongoIncompatibleDriverException exception = createIncompatibleException(clusterDescription);
        logServerSelectionFailed(clusterId, operationId, exception, serverSelector, clusterDescription);
        throw exception;
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

    private void logAndThrowTimeoutException(
            final long operationId,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        MongoTimeoutException exception = new MongoTimeoutException(format(
                "Timed out while waiting for a server that matches %s. Client view of cluster state is %s",
                serverSelector, clusterDescription.getShortDescription()));
        logServerSelectionFailed(clusterId, operationId, exception, serverSelector, clusterDescription);
        throw exception;
    }

    private static final class ServerSelectionRequest {
        private final ServerSelector originalSelector;
        private final ServerSelector compositeSelector;
        private final SingleResultCallback<ServerTuple> callback;
        private final long operationId;
        private final Timeout timeout;
        private CountDownLatch phase;

        ServerSelectionRequest(final ServerSelector serverSelector, final ServerSelector compositeSelector,
                final long operationId, final Timeout timeout, final SingleResultCallback<ServerTuple> callback) {
            this.originalSelector = serverSelector;
            this.compositeSelector = compositeSelector;
            this.operationId = operationId;
            this.timeout = timeout;
            this.callback = callback;
        }

        void onResult(@Nullable final ServerTuple serverTuple, @Nullable final Throwable t) {
            try {
                callback.onResult(serverTuple, t);
            } catch (Throwable tr) {
                // ignore
            }
        }

        Timeout getTimeout() {
            return timeout;
        }

        public long getOperationId() {
            return operationId;
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

        WaitQueueHandler() {
        }

        public void run() {
            while (!isClosed) {
                CountDownLatch currentPhase = phase.get();
                ClusterDescription curDescription = description;

                Timeout timeout = Timeout.infinite();
                boolean someWaitersNotSatisfied = false;
                for (Iterator<ServerSelectionRequest> iter = waitQueue.iterator(); iter.hasNext();) {
                    ServerSelectionRequest currentRequest = iter.next();
                    if (handleServerSelectionRequest(currentRequest, currentPhase, curDescription)) {
                        iter.remove();
                    } else {
                        someWaitersNotSatisfied = true;
                        timeout = timeout
                                .orEarlier(currentRequest.getTimeout())
                                .orEarlier(startMinWaitHeartbeatTimeout());
                    }
                }

                // TODO-CSOT because min heartbeat cannot be infinite, infinite is being used to mark the second branch
                if (someWaitersNotSatisfied) {
                    connect();
                }

                try {
                    timeout.awaitOn(currentPhase, () -> "ignored");
                } catch (MongoInterruptedException closed) {
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

    static void logServerSelectionStarted(
            final ClusterId clusterId,
            final long operationId,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, DEBUG, "Server selection started", clusterId,
                    asList(
                            new Entry(OPERATION, null),
                            new Entry(OPERATION_ID, operationId),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Server selection started for operation[ {}] with ID {}. Selector: {}, topology description: {}"));
        }
    }

    private static void logServerSelectionWaiting(
            final ClusterId clusterId,
            final long operationId,
            final Timeout timeout,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(INFO, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, INFO, "Waiting for suitable server to become available", clusterId,
                    asList(
                            new Entry(OPERATION, null),
                            new Entry(OPERATION_ID, operationId),
                            new Entry(REMAINING_TIME_MS, timeout.run(MILLISECONDS, () -> "infinite", (ms) -> ms, () -> 0L)),
                            // TODO-CSOT the above extracts values, but the alternative seems worse
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Waiting for server to become available for operation[ {}] with ID {}.[ Remaining time: {} ms.]"
                            + " Selector: {}, topology description: {}."));
        }
    }

    private static void logServerSelectionFailed(
            final ClusterId clusterId,
            final long operationId,
            final MongoException failure,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            String failureDescription = failure instanceof MongoTimeoutException
                    // This hardcoded message guarantees that the `FAILURE` entry for `MongoTimeoutException` does not include
                    // any information that is specified via other entries, e.g., `SELECTOR` and `TOPOLOGY_DESCRIPTION`.
                    // The logging spec requires us to avoid such duplication of information.
                    ? MongoTimeoutException.class.getName() + ": Timed out while waiting for a suitable server"
                    : failure.toString();
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, DEBUG, "Server selection failed", clusterId,
                    asList(
                            new Entry(OPERATION, null),
                            new Entry(OPERATION_ID, operationId),
                            new Entry(FAILURE, failureDescription),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Server selection failed for operation[ {}] with ID {}. Failure: {}. Selector: {}, topology description: {}"));
        }
    }

    static void logServerSelectionSucceeded(
            final ClusterId clusterId,
            final long operationId,
            final ServerAddress serverAddress,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, DEBUG, "Server selection succeeded", clusterId,
                    asList(
                            new Entry(OPERATION, null),
                            new Entry(OPERATION_ID, operationId),
                            new Entry(SERVER_HOST, serverAddress.getHost()),
                            new Entry(SERVER_PORT, serverAddress instanceof UnixServerAddress ? null : serverAddress.getPort()),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Server selection succeeded for operation[ {}] with ID {}. Selected server: {}[:{}]."
                            + " Selector: {}, topology description: {}"));
        }
    }
}

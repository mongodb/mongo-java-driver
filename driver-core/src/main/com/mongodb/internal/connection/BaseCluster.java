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
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.OperationContext.ServerDeprioritization;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.logging.LogMessage.Entry;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.selector.AtMostTwoRandomServerSelector;
import com.mongodb.internal.selector.LatencyMinimizingServerSelector;
import com.mongodb.internal.selector.MinimumOperationCountServerSelector;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_SERVER_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.internal.Locks.withInterruptibleLock;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PROTECTED;
import static com.mongodb.internal.connection.EventHelper.wouldDescriptionsGenerateEquivalentEvents;
import static com.mongodb.internal.event.EventListenerHelper.singleClusterListener;
import static com.mongodb.internal.logging.LogMessage.Component.SERVER_SELECTION;
import static com.mongodb.internal.logging.LogMessage.Component.TOPOLOGY;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.FAILURE;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.OPERATION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.OPERATION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REMAINING_TIME_MS;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SELECTOR;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_HOST;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_PORT;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.TOPOLOGY_DESCRIPTION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.TOPOLOGY_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.TOPOLOGY_NEW_DESCRIPTION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.TOPOLOGY_PREVIOUS_DESCRIPTION;
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;
import static com.mongodb.internal.logging.LogMessage.Level.INFO;
import static com.mongodb.internal.time.Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

@VisibleForTesting(otherwise = PRIVATE)
public abstract class BaseCluster implements Cluster {
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
    private final ClientMetadata clientMetadata;
    private Thread waitQueueHandler;

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    @VisibleForTesting(otherwise = PRIVATE)
    protected BaseCluster(final ClusterId clusterId,
                          final ClusterSettings settings,
                          final ClusterableServerFactory serverFactory,
                          final ClientMetadata clientMetadata) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        this.serverFactory = notNull("serverFactory", serverFactory);
        this.clusterListener = singleClusterListener(settings);
        this.description = new ClusterDescription(settings.getMode(), UNKNOWN, emptyList(),
                settings, serverFactory.getSettings());
        this.clientMetadata = clientMetadata;
        logTopologyMonitoringStarting(clusterId);
        ClusterOpeningEvent clusterOpeningEvent = new ClusterOpeningEvent(clusterId);
        clusterListener.clusterOpening(clusterOpeningEvent);
    }

    @Override
    public ClusterClock getClock() {
        return clusterClock;
    }

    @Override
    public ClientMetadata getClientMetadata() {
        return clientMetadata;
    }

    @Override
    public ServerTuple selectServer(final ServerSelector serverSelector, final OperationContext operationContext) {
        isTrue("open", !isClosed());

        ServerDeprioritization serverDeprioritization = operationContext.getServerDeprioritization();
        boolean selectionWaitingLogged = false;
        Timeout computedServerSelectionTimeout = operationContext.getTimeoutContext().computeServerSelectionTimeout();
        logServerSelectionStarted(operationContext, clusterId, serverSelector, description);
        while (true) {
            CountDownLatch currentPhaseLatch = phase.get();
            ClusterDescription currentDescription = description;
            ServerTuple serverTuple = createCompleteSelectorAndSelectServer(
                    serverSelector, currentDescription, serverDeprioritization,
                    computedServerSelectionTimeout, operationContext.getTimeoutContext());

            if (!currentDescription.isCompatibleWithDriver()) {
                logAndThrowIncompatibleException(operationContext, serverSelector, currentDescription);
            }
            if (serverTuple != null) {
                ServerAddress serverAddress = serverTuple.getServerDescription().getAddress();
                logServerSelectionSucceeded(operationContext, clusterId, serverAddress, serverSelector, currentDescription);
                serverDeprioritization.updateCandidate(serverAddress);
                return serverTuple;
            }
            computedServerSelectionTimeout.onExpired(() ->
                    logAndThrowTimeoutException(operationContext, serverSelector, currentDescription));

            if (!selectionWaitingLogged) {
                logServerSelectionWaiting(operationContext, clusterId, computedServerSelectionTimeout, serverSelector, currentDescription);
                selectionWaitingLogged = true;
            }
            connect();

            Timeout heartbeatLimitedTimeout = Timeout.earliest(
                    computedServerSelectionTimeout,
                    startMinWaitHeartbeatTimeout());

            heartbeatLimitedTimeout.awaitOn(currentPhaseLatch,
                    () -> format("waiting for a server that matches %s", serverSelector));
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final OperationContext operationContext,
            final SingleResultCallback<ServerTuple> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoClientException("Cluster was closed during server selection."));
        }

        Timeout computedServerSelectionTimeout = operationContext.getTimeoutContext().computeServerSelectionTimeout();
        ServerSelectionRequest request = new ServerSelectionRequest(
                serverSelector, operationContext, computedServerSelectionTimeout, callback);

        CountDownLatch currentPhase = phase.get();
        ClusterDescription currentDescription = description;

        logServerSelectionStarted(operationContext, clusterId, serverSelector, currentDescription);

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
            fireChangeEvent(new ClusterDescription(settings.getMode(), UNKNOWN, emptyList(), settings, serverFactory.getSettings()),
                    description);
            logTopologyMonitoringStopping(clusterId);
            ClusterClosedEvent clusterClosedEvent = new ClusterClosedEvent(clusterId);
            clusterListener.clusterClosed(clusterClosedEvent);
            stopWaitQueueHandler();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @VisibleForTesting(otherwise = PROTECTED)
    public void updateDescription(final ClusterDescription newDescription) {
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
            ClusterDescriptionChangedEvent changedEvent = new ClusterDescriptionChangedEvent(getClusterId(), newDescription, previousDescription);
            logTopologyDescriptionChanged(getClusterId(),  changedEvent);
            clusterListener.clusterDescriptionChanged(changedEvent);
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
        return Timeout.expiresIn(minHeartbeatFrequency, NANOSECONDS, ZERO_DURATION_MEANS_EXPIRED);
    }

    private boolean handleServerSelectionRequest(
            final ServerSelectionRequest request, final CountDownLatch currentPhase,
            final ClusterDescription description) {

        try {
            OperationContext operationContext = request.getOperationContext();
            if (currentPhase != request.phase) {
                CountDownLatch prevPhase = request.phase;
                request.phase = currentPhase;
                if (!description.isCompatibleWithDriver()) {
                    logAndThrowIncompatibleException(operationContext, request.originalSelector, description);
                }


                ServerDeprioritization serverDeprioritization = request.operationContext.getServerDeprioritization();
                ServerTuple serverTuple = createCompleteSelectorAndSelectServer(
                        request.originalSelector,
                        description,
                        serverDeprioritization,
                        request.getTimeout(),
                        operationContext.getTimeoutContext());

                if (serverTuple != null) {
                    ServerAddress serverAddress = serverTuple.getServerDescription().getAddress();
                    logServerSelectionSucceeded(operationContext, clusterId, serverAddress, request.originalSelector, description);
                    serverDeprioritization.updateCandidate(serverAddress);
                    request.onResult(serverTuple, null);
                    return true;
                }
                if (prevPhase == null) {
                    logServerSelectionWaiting(operationContext, clusterId, request.getTimeout(), request.originalSelector, description);
                }
            }

            Timeout.onExistsAndExpired(request.getTimeout(), () -> {
                logAndThrowTimeoutException(operationContext, request.originalSelector, description);
            });
            return false;
        } catch (Exception e) {
            request.onResult(null, e);
            return true;
        }
    }

    @Nullable
    private ServerTuple createCompleteSelectorAndSelectServer(
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription,
            final ServerDeprioritization serverDeprioritization,
            final Timeout serverSelectionTimeout,
            final TimeoutContext timeoutContext) {
        return createCompleteSelectorAndSelectServer(
                serverSelector,
                clusterDescription,
                getServersSnapshot(serverSelectionTimeout, timeoutContext),
                serverDeprioritization,
                settings);
    }

    @Nullable
    @VisibleForTesting(otherwise = PRIVATE)
    static ServerTuple createCompleteSelectorAndSelectServer(
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription,
            final ServersSnapshot serversSnapshot,
            final ServerDeprioritization serverDeprioritization,
            final ClusterSettings settings) {
        ServerSelector completeServerSelector = getCompleteServerSelector(serverSelector, serverDeprioritization, serversSnapshot, settings);
        return completeServerSelector.select(clusterDescription)
                .stream()
                .map(serverDescription -> new ServerTuple(
                        assertNotNull(serversSnapshot.getServer(serverDescription.getAddress())),
                        serverDescription))
                .findAny()
                .orElse(null);
    }

    private static ServerSelector getCompleteServerSelector(
            final ServerSelector serverSelector,
            final ServerDeprioritization serverDeprioritization,
            final ServersSnapshot serversSnapshot,
            final ClusterSettings settings) {
        List<ServerSelector> selectors = Stream.of(
                getRaceConditionPreFilteringSelector(serversSnapshot),
                serverDeprioritization.applyDeprioritization(serverSelector),
                settings.getServerSelector(), // may be null
                new LatencyMinimizingServerSelector(settings.getLocalThreshold(MILLISECONDS), MILLISECONDS),
                AtMostTwoRandomServerSelector.instance(),
                new MinimumOperationCountServerSelector(serversSnapshot)
        ).filter(Objects::nonNull).collect(toList());
        return new CompositeServerSelector(selectors);
    }

    private static ServerSelector getRaceConditionPreFilteringSelector(final ServersSnapshot serversSnapshot) {
        // The set of `Server`s maintained by the `Cluster` is updated concurrently with `clusterDescription` being read.
        // Additionally, that set of servers continues to be concurrently updated while `serverSelector` selects.
        // This race condition means that we are not guaranteed to observe all the servers from `clusterDescription`
        // among the `Server`s maintained by the `Cluster`.
        // To deal with this race condition, we take `serversSnapshot` of that set of `Server`s
        // (the snapshot itself does not have to be atomic) non-atomically with reading `clusterDescription`
        // (this means, `serversSnapshot` and `clusterDescription` are not guaranteed to be consistent with each other),
        // and do pre-filtering to make sure that the only `ServerDescription`s we may select,
        // are of those `Server`s that are known to both `clusterDescription` and `serversSnapshot`.
        return clusterDescription -> clusterDescription.getServerDescriptions()
                .stream()
                .filter(serverDescription -> serversSnapshot.containsServer(serverDescription.getAddress()))
                .collect(toList());
    }

    protected ClusterableServer createServer(final ServerAddress serverAddress) {
        return serverFactory.create(this, serverAddress);
    }

    private void logAndThrowIncompatibleException(
            final OperationContext operationContext,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        MongoIncompatibleDriverException exception = createIncompatibleException(clusterDescription);
        logServerSelectionFailed(operationContext, clusterId, exception, serverSelector, clusterDescription);
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
            final OperationContext operationContext,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        String message = format(
                "Timed out while waiting for a server that matches %s. Client view of cluster state is %s",
                serverSelector, clusterDescription.getShortDescription());

        MongoTimeoutException exception = operationContext.getTimeoutContext().hasTimeoutMS()
                ? new MongoOperationTimeoutException(message) : new MongoTimeoutException(message);

        logServerSelectionFailed(operationContext, clusterId, exception, serverSelector, clusterDescription);
        throw exception;
    }

    private static final class ServerSelectionRequest {
        private final ServerSelector originalSelector;
        private final SingleResultCallback<ServerTuple> callback;
        private final OperationContext operationContext;
        private final Timeout timeout;
        private CountDownLatch phase;

        ServerSelectionRequest(
                final ServerSelector serverSelector,
                final OperationContext operationContext,
                final Timeout timeout,
                final SingleResultCallback<ServerTuple> callback) {
            this.originalSelector = serverSelector;
            this.operationContext = operationContext;
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

        public OperationContext getOperationContext() {
            return operationContext;
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
            try {
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
                            timeout = Timeout.earliest(
                                    timeout,
                                    currentRequest.getTimeout(),
                                    startMinWaitHeartbeatTimeout());
                        }
                    }

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
            } catch (Throwable t) {
                LOGGER.error(this + " stopped working. You may want to recreate the MongoClient", t);
                throw t;
            }
        }
    }

    static void logServerSelectionStarted(
            final OperationContext operationContext,
            final ClusterId clusterId,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, DEBUG, "Server selection started", clusterId,
                    asList(
                            new Entry(OPERATION, operationContext.getOperationName()),
                            new Entry(OPERATION_ID, operationContext.getId()),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Server selection started for operation[ {}] with ID {}. Selector: {}, topology description: {}"));
        }
    }

    private static void logServerSelectionWaiting(
            final OperationContext operationContext,
            final ClusterId clusterId,
            final Timeout timeout,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(INFO, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, INFO, "Waiting for suitable server to become available", clusterId,
                    asList(
                            new Entry(OPERATION, operationContext.getOperationName()),
                            new Entry(OPERATION_ID, operationContext.getId()),
                            timeout.call(MILLISECONDS,
                                    () -> new Entry(REMAINING_TIME_MS, "infinite"),
                                    (ms) -> new Entry(REMAINING_TIME_MS, ms),
                                    () -> new Entry(REMAINING_TIME_MS, 0L)),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Waiting for server to become available for operation[ {}] with ID {}.[ Remaining time: {} ms.]"
                            + " Selector: {}, topology description: {}."));
        }
    }

    private static void logServerSelectionFailed(
            final OperationContext operationContext,
            final ClusterId clusterId,
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
                            new Entry(OPERATION, operationContext.getOperationName()),
                            new Entry(OPERATION_ID, operationContext.getId()),
                            new Entry(FAILURE, failureDescription),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Server selection failed for operation[ {}] with ID {}. Failure: {}. Selector: {}, topology description: {}"));
        }
    }

    static void logServerSelectionSucceeded(
            final OperationContext operationContext,
            final ClusterId clusterId,
            final ServerAddress serverAddress,
            final ServerSelector serverSelector,
            final ClusterDescription clusterDescription) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    SERVER_SELECTION, DEBUG, "Server selection succeeded", clusterId,
                    asList(
                            new Entry(OPERATION, operationContext.getOperationName()),
                            new Entry(OPERATION_ID, operationContext.getId()),
                            new Entry(SERVER_HOST, serverAddress.getHost()),
                            new Entry(SERVER_PORT, serverAddress instanceof UnixServerAddress ? null : serverAddress.getPort()),
                            new Entry(SELECTOR, serverSelector.toString()),
                            new Entry(TOPOLOGY_DESCRIPTION, clusterDescription.getShortDescription())),
                    "Server selection succeeded for operation[ {}] with ID {}. Selected server: {}[:{}]."
                            + " Selector: {}, topology description: {}"));
        }
    }

    static void logTopologyMonitoringStarting(final ClusterId clusterId) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    TOPOLOGY, DEBUG, "Starting topology monitoring", clusterId,
                    singletonList(new Entry(TOPOLOGY_ID, clusterId)),
                    "Starting monitoring for topology with ID {}"));
        }
    }

    static void logTopologyDescriptionChanged(
            final ClusterId clusterId,
            final ClusterDescriptionChangedEvent clusterDescriptionChangedEvent) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    TOPOLOGY, DEBUG, "Topology description changed", clusterId,
                    asList(
                            new Entry(TOPOLOGY_ID, clusterId),
                            new Entry(TOPOLOGY_PREVIOUS_DESCRIPTION,
                                    clusterDescriptionChangedEvent.getPreviousDescription().getShortDescription()),
                            new Entry(TOPOLOGY_NEW_DESCRIPTION,
                                    clusterDescriptionChangedEvent.getNewDescription().getShortDescription())),
                    "Description changed for topology with ID {}. Previous description: {}. New description: {}"));
        }
    }

    static void logTopologyMonitoringStopping(final ClusterId clusterId) {
        if (STRUCTURED_LOGGER.isRequired(DEBUG, clusterId)) {
            STRUCTURED_LOGGER.log(new LogMessage(
                    TOPOLOGY, DEBUG, "Stopped topology monitoring", clusterId,
                    singletonList(new Entry(TOPOLOGY_ID, clusterId)),
                    "Stopped monitoring for topology with ID {}"));
        }
    }

}

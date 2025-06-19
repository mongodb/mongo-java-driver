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
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.internal.Locks;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.connection.BaseCluster.logServerSelectionStarted;
import static com.mongodb.internal.connection.BaseCluster.logServerSelectionSucceeded;
import static com.mongodb.internal.connection.BaseCluster.logTopologyMonitoringStopping;
import static com.mongodb.internal.event.EventListenerHelper.singleClusterListener;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
final class LoadBalancedCluster implements Cluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ClusterId clusterId;
    private final ClusterSettings settings;
    private final ClusterClock clusterClock = new ClusterClock();
    private final ClusterListener clusterListener;
    private ClusterDescription description;
    @Nullable
    private ClusterableServer server;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final DnsSrvRecordMonitor dnsSrvRecordMonitor;
    private volatile MongoException srvResolutionException;
    private boolean srvRecordResolvedToMultipleHosts;
    private volatile boolean initializationCompleted;
    private List<ServerSelectionRequest> waitQueue = new LinkedList<>();
    private Thread waitQueueHandler;
    private final Lock lock = new ReentrantLock(true);
    private final Condition condition = lock.newCondition();

    LoadBalancedCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                        final DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory) {
        assertTrue(settings.getMode() == ClusterConnectionMode.LOAD_BALANCED);
        LOGGER.info(format("Cluster created with id %s and settings %s", clusterId, settings.getShortDescription()));

        this.clusterId = clusterId;
        this.settings = settings;
        this.clusterListener = singleClusterListener(settings);
        this.description = new ClusterDescription(settings.getMode(), ClusterType.UNKNOWN, emptyList(), settings,
                serverFactory.getSettings());

        if (settings.getSrvHost() == null) {
            dnsSrvRecordMonitor = null;
            init(clusterId, serverFactory, settings.getHosts().get(0));
            initializationCompleted = true;
        } else {
            notNull("dnsSrvRecordMonitorFactory", dnsSrvRecordMonitorFactory);
            dnsSrvRecordMonitor = dnsSrvRecordMonitorFactory.create(assertNotNull(settings.getSrvHost()), settings.getSrvServiceName(),
                    new DnsSrvRecordInitializer() {

                @Override
                public void initialize(final Collection<ServerAddress> hosts) {
                    LOGGER.info("SRV resolution completed with hosts: " + hosts);

                    List<ServerSelectionRequest> localWaitQueue;
                    lock.lock();
                    try {
                        if (isClosed()) {
                            return;
                        }
                        srvResolutionException = null;
                        if (hosts.size() != 1) {
                            srvRecordResolvedToMultipleHosts = true;
                        } else {
                            init(clusterId, serverFactory, hosts.iterator().next());
                        }
                        initializationCompleted = true;
                        localWaitQueue = waitQueue;
                        waitQueue = emptyList();
                        condition.signalAll();
                    } finally {
                        lock.unlock();
                    }
                    localWaitQueue.forEach(request -> handleServerSelectionRequest(request));
                }

                @Override
                public void initialize(final MongoException initializationException) {
                    srvResolutionException = initializationException;
                }

                @Override
                public ClusterType getClusterType() {
                    return initializationCompleted ? ClusterType.LOAD_BALANCED : ClusterType.UNKNOWN;
                }
            });
            dnsSrvRecordMonitor.start();
        }
    }

    private void init(final ClusterId clusterId, final ClusterableServerFactory serverFactory, final ServerAddress host) {
        clusterListener.clusterOpening(new ClusterOpeningEvent(clusterId));

        ClusterDescription initialDescription = new ClusterDescription(settings.getMode(), ClusterType.LOAD_BALANCED,
                singletonList(ServerDescription.builder().address(settings.getHosts().get(0)).state(CONNECTING).build()),
                settings, serverFactory.getSettings());
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, initialDescription, description));

        description = new ClusterDescription(ClusterConnectionMode.LOAD_BALANCED, ClusterType.LOAD_BALANCED,
                singletonList(ServerDescription.builder()
                        .ok(true)
                        .state(ServerConnectionState.CONNECTED)
                        .type(ServerType.LOAD_BALANCER)
                        .address(host)
                        .build()),
                settings, serverFactory.getSettings());
        server = serverFactory.create(this, host);

        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, description, initialDescription));
    }

    @Override
    public ClusterSettings getSettings() {
        isTrue("open", !isClosed());
        return settings;
    }

    @Override
    public ClusterId getClusterId() {
        return clusterId;
    }

    @Override
    public ServersSnapshot getServersSnapshot(
            final Timeout serverSelectionTimeout,
            final TimeoutContext timeoutContext) {
        isTrue("open", !isClosed());
        waitForSrv(serverSelectionTimeout, timeoutContext);
        ClusterableServer server = assertNotNull(this.server);
        return serverAddress -> server;
    }

    @Override
    public ClusterDescription getCurrentDescription() {
        isTrue("open", !isClosed());
        return description;
    }

    @Override
    public ClusterClock getClock() {
        isTrue("open", !isClosed());
        return clusterClock;
    }

    @Override
    public ServerTuple selectServer(final ServerSelector serverSelector, final OperationContext operationContext) {
        isTrue("open", !isClosed());
        Timeout computedServerSelectionTimeout = operationContext.getTimeoutContext().computeServerSelectionTimeout();
        waitForSrv(computedServerSelectionTimeout, operationContext.getTimeoutContext());
        if (srvRecordResolvedToMultipleHosts) {
            throw createResolvedToMultipleHostsException();
        }
        ClusterDescription curDescription = description;
        logServerSelectionStarted(clusterId, operationContext.getId(), serverSelector, curDescription);
        ServerTuple serverTuple = new ServerTuple(assertNotNull(server), curDescription.getServerDescriptions().get(0));
        logServerSelectionSucceeded(clusterId, operationContext.getId(), serverTuple.getServerDescription().getAddress(),
                serverSelector, curDescription);
        return serverTuple;
    }

    private void waitForSrv(final Timeout serverSelectionTimeout, final TimeoutContext timeoutContext) {
        if (initializationCompleted) {
            return;
        }
        Locks.withLock(lock, () -> {
            while (!initializationCompleted) {
                if (isClosed()) {
                    throw createShutdownException();
                }
                serverSelectionTimeout.onExpired(() -> {
                    throw createTimeoutException(timeoutContext);
                });
                serverSelectionTimeout.awaitOn(condition, () -> format("resolving SRV records for %s", settings.getSrvHost()));
            }
        });
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final OperationContext operationContext,
            final SingleResultCallback<ServerTuple> callback) {
        if (isClosed()) {
            callback.onResult(null, createShutdownException());
            return;
        }
        Timeout computedServerSelectionTimeout = operationContext.getTimeoutContext().computeServerSelectionTimeout();
        ServerSelectionRequest serverSelectionRequest = new ServerSelectionRequest(operationContext.getId(), serverSelector,
                operationContext, computedServerSelectionTimeout, callback);
        if (initializationCompleted) {
            handleServerSelectionRequest(serverSelectionRequest);
        } else {
            notifyWaitQueueHandler(serverSelectionRequest);
        }
    }

    private MongoClientException createShutdownException() {
        return new MongoClientException("Shutdown in progress");
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            LOGGER.info(format("Cluster closed with id %s", clusterId));
            if (dnsSrvRecordMonitor != null) {
                dnsSrvRecordMonitor.close();
            }
            ClusterableServer localServer = Locks.withLock(lock, () -> {
                condition.signalAll();
                return server;
            });
            if (localServer != null) {
                localServer.close();
            }
            ClusterClosedEvent clusterClosedEvent = new ClusterClosedEvent(clusterId);
            clusterListener.clusterClosed(clusterClosedEvent);
            logTopologyMonitoringStopping(clusterId);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void withLock(final Runnable action) {
        fail();
    }

    @Override
    public void onChange(final ServerDescriptionChangedEvent event) {
        fail();
    }

    private void handleServerSelectionRequest(final ServerSelectionRequest serverSelectionRequest) {
        assertTrue(initializationCompleted);
        if (srvRecordResolvedToMultipleHosts) {
            serverSelectionRequest.onError(createResolvedToMultipleHostsException());
        } else {
            ClusterDescription curDescription = description;
            logServerSelectionStarted(
                    clusterId, serverSelectionRequest.operationId, serverSelectionRequest.serverSelector, curDescription);
            ServerTuple serverTuple = new ServerTuple(assertNotNull(server), curDescription.getServerDescriptions().get(0));
            logServerSelectionSucceeded(clusterId, serverSelectionRequest.operationId,
                    serverTuple.getServerDescription().getAddress(), serverSelectionRequest.serverSelector, curDescription);
            serverSelectionRequest.onSuccess(serverTuple);
        }
    }

    private MongoClientException createResolvedToMultipleHostsException() {
        return new MongoClientException("In load balancing mode, the host must resolve to a single SRV record, but instead it resolved "
                + "to multiple hosts");
    }

    private MongoTimeoutException createTimeoutException(final TimeoutContext timeoutContext) {
        MongoException localSrvResolutionException = srvResolutionException;
        String message;
        if (localSrvResolutionException == null) {
            message = format("Timed out while waiting to resolve SRV records for %s.", settings.getSrvHost());
        } else {
            message = format("Timed out while waiting to resolve SRV records for %s. "
                    + "Resolution exception was '%s'", settings.getSrvHost(), localSrvResolutionException);
        }
        return createTimeoutException(timeoutContext, message);
    }

    private static MongoTimeoutException createTimeoutException(final TimeoutContext timeoutContext, final String message) {
        return timeoutContext.hasTimeoutMS() ? new MongoOperationTimeoutException(message) : new MongoTimeoutException(message);
    }

    private void notifyWaitQueueHandler(final ServerSelectionRequest request) {
        Locks.withLock(lock, () ->  {
            if (isClosed()) {
                request.onError(createShutdownException());
                return;
            }
            if (initializationCompleted) {
                handleServerSelectionRequest(request);
                return;
            }

            waitQueue.add(request);

            if (waitQueueHandler == null) {
                waitQueueHandler = new Thread(new WaitQueueHandler(), "cluster-" + clusterId.getValue());
                waitQueueHandler.setDaemon(true);
                waitQueueHandler.start();
            } else {
                condition.signalAll();
            }
        });
    }

    private final class WaitQueueHandler implements Runnable {
        public void run() {
            List<ServerSelectionRequest> timeoutList = new ArrayList<>();
            while (!(isClosed() || initializationCompleted)) {
                lock.lock();
                try {
                    if (isClosed() || initializationCompleted) {
                        break;
                    }
                    Timeout waitTimeNanos = Timeout.infinite();

                    for (Iterator<ServerSelectionRequest> iterator = waitQueue.iterator(); iterator.hasNext();) {
                        ServerSelectionRequest next = iterator.next();

                        Timeout nextTimeout = next.getTimeout();
                        Timeout waitTimeNanosFinal = waitTimeNanos;
                        waitTimeNanos = nextTimeout.call(NANOSECONDS,
                                () -> Timeout.earliest(waitTimeNanosFinal, nextTimeout),
                                (ns) -> Timeout.earliest(waitTimeNanosFinal, nextTimeout),
                                () -> {
                                    timeoutList.add(next);
                                    iterator.remove();
                                    return waitTimeNanosFinal;
                                });
                    }
                    if (timeoutList.isEmpty()) {
                        try {
                            waitTimeNanos.awaitOn(condition, () -> "ignored");
                        } catch (MongoInterruptedException unexpected) {
                            fail();
                        }
                    }
                } finally {
                    lock.unlock();
                }
                timeoutList.forEach(request -> request.onError(createTimeoutException(request
                        .getOperationContext()
                        .getTimeoutContext())));
                timeoutList.clear();
            }

            // This code is executed either after closing the LoadBalancedCluster or after initializing it. In the latter case,
            // waitQueue is guaranteed to be empty (as DnsSrvRecordInitializer.initialize clears it and no thread adds new elements to
            // it after that). So shutdownList is not empty iff LoadBalancedCluster is closed, in which case we need to complete the
            // requests in it.
            List<ServerSelectionRequest> shutdownList = Locks.withLock(lock, () -> {
                ArrayList<ServerSelectionRequest> result = new ArrayList<>(waitQueue);
                waitQueue.clear();
                return result;
            });
            shutdownList.forEach(request -> request.onError(createShutdownException()));
        }
    }

    private static final class ServerSelectionRequest {
        private final long operationId;
        private final ServerSelector serverSelector;
        private final SingleResultCallback<ServerTuple> callback;
        private final Timeout timeout;
        private final OperationContext operationContext;

        private ServerSelectionRequest(final long operationId, final ServerSelector serverSelector, final OperationContext operationContext,
                                       final Timeout timeout, final SingleResultCallback<ServerTuple> callback) {
            this.operationId = operationId;
            this.serverSelector = serverSelector;
            this.timeout = timeout;
            this.operationContext = operationContext;
            this.callback = callback;
        }

        Timeout getTimeout() {
            return timeout;
        }

        OperationContext getOperationContext() {
            return operationContext;
        }

        public void onSuccess(final ServerTuple serverTuple) {
            try {
                callback.onResult(serverTuple, null);
            } catch (Exception e) {
                LOGGER.warn("Unanticipated exception thrown from callback", e);
            }
        }

        public void onError(final Throwable exception) {
            try {
                callback.onResult(null, exception);
            } catch (Exception e) {
                LOGGER.warn("Unanticipated exception thrown from callback", e);
            }
        }
    }
}

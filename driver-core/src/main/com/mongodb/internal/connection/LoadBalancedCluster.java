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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
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
import static com.mongodb.internal.event.EventListenerHelper.singleClusterListener;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    public ClusterDescription getDescription() {
        isTrue("open", !isClosed());
        waitForSrv();
        return description;
    }

    @Override
    public ClusterId getClusterId() {
        return clusterId;
    }

    @Override
    public ClusterableServer getServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());
        waitForSrv();
        return assertNotNull(server);
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
    public ServerTuple selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        waitForSrv();
        if (srvRecordResolvedToMultipleHosts) {
            throw createResolvedToMultipleHostsException();
        }
        return new ServerTuple(assertNotNull(server), description.getServerDescriptions().get(0));
    }


    private void waitForSrv() {
        if (initializationCompleted) {
            return;
        }
        lock.lock();
        try {
            long remainingTimeNanos = getMaxWaitTimeNanos();
            while (!initializationCompleted) {
                if (isClosed()) {
                    throw createShutdownException();
                }
                if (remainingTimeNanos <= 0) {
                    throw createTimeoutException();
                }
                remainingTimeNanos = condition.awaitNanos(remainingTimeNanos);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MongoInterruptedException(format("Interrupted while resolving SRV records for %s", settings.getSrvHost()), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<ServerTuple> callback) {
        if (isClosed()) {
            callback.onResult(null, createShutdownException());
            return;
        }

        ServerSelectionRequest serverSelectionRequest = new ServerSelectionRequest(getMaxWaitTimeNanos(), callback);
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
            ClusterableServer localServer;
            lock.lock();
            try {
                condition.signalAll();
                localServer = server;
            } finally {
                lock.unlock();
            }
            if (localServer != null) {
                localServer.close();
            }
            clusterListener.clusterClosed(new ClusterClosedEvent(clusterId));
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
            serverSelectionRequest.onSuccess(new ServerTuple(assertNotNull(server), description.getServerDescriptions().get(0)));
        }
    }

    private MongoClientException createResolvedToMultipleHostsException() {
        return new MongoClientException("In load balancing mode, the host must resolve to a single SRV record, but instead it resolved "
                + "to multiple hosts");
    }

    private MongoTimeoutException createTimeoutException() {
        MongoException localSrvResolutionException = srvResolutionException;
        if (localSrvResolutionException == null) {
            return new MongoTimeoutException(format("Timed out after %d ms while waiting to resolve SRV records for %s.",
                    settings.getServerSelectionTimeout(MILLISECONDS), settings.getSrvHost()));
        } else {
            return new MongoTimeoutException(format("Timed out after %d ms while waiting to resolve SRV records for %s. "
                            + "Resolution exception was '%s'",
                    settings.getServerSelectionTimeout(MILLISECONDS), settings.getSrvHost(), localSrvResolutionException));
        }
    }

    private long getMaxWaitTimeNanos() {
        if (settings.getServerSelectionTimeout(NANOSECONDS) < 0) {
            return Long.MAX_VALUE;
        }
        return settings.getServerSelectionTimeout(NANOSECONDS);
    }

    private void notifyWaitQueueHandler(final ServerSelectionRequest request) {
        lock.lock();
        try {
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
        } finally {
            lock.unlock();
        }
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
                    long waitTimeNanos = Long.MAX_VALUE;
                    long curTimeNanos = System.nanoTime();

                    for (Iterator<ServerSelectionRequest> iterator = waitQueue.iterator(); iterator.hasNext();) {
                        ServerSelectionRequest next = iterator.next();
                        long remainingTime = next.getRemainingTime(curTimeNanos);
                        if (remainingTime <= 0) {
                            timeoutList.add(next);
                            iterator.remove();
                        } else {
                            waitTimeNanos = Math.min(remainingTime, waitTimeNanos);
                        }
                    }
                    if (timeoutList.isEmpty()) {
                        try {
                            //noinspection ResultOfMethodCallIgnored
                            condition.await(waitTimeNanos, NANOSECONDS);
                        } catch (InterruptedException e) {
                            fail();
                        }
                    }
                } finally {
                    lock.unlock();
                }

                timeoutList.forEach(request -> request.onError(createTimeoutException()));
                timeoutList.clear();
            }

            // This code is executed either after closing the LoadBalancedCluster or after initializing it. In the latter case,
            // waitQueue is guaranteed to be empty (as DnsSrvRecordInitializer.initialize clears it and no thread adds new elements to
            // it after that). So shutdownList is not empty iff LoadBalancedCluster is closed, in which case we need to complete the
            // requests in it.
            List<ServerSelectionRequest> shutdownList;
            lock.lock();
            try {
                shutdownList = new ArrayList<>(waitQueue);
                waitQueue.clear();
            } finally {
                lock.unlock();
            }
            shutdownList.forEach(request -> request.onError(createShutdownException()));
        }
    }

    private static final class ServerSelectionRequest {
        private final long maxWaitTimeNanos;
        private final long startTimeNanos = System.nanoTime();
        private final SingleResultCallback<ServerTuple> callback;

        private ServerSelectionRequest(final long maxWaitTimeNanos, final SingleResultCallback<ServerTuple> callback) {
            this.maxWaitTimeNanos = maxWaitTimeNanos;
            this.callback = callback;
        }

        long getRemainingTime(final long curTimeNanos) {
            return startTimeNanos + maxWaitTimeNanos - curTimeNanos;
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

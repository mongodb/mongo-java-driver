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

import com.mongodb.MongoClientException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWaitQueueFullException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.connection.ConcurrentLinkedDeque;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final ClusterId clusterId;
    private final ClusterSettings settings;
    private final ClusterListener clusterListener;
    private final Deque<ServerSelectionRequest> waitQueue = new ConcurrentLinkedDeque<ServerSelectionRequest>();
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private Thread waitQueueHandler;

    private volatile boolean isClosed;
    private volatile ClusterDescription description;

    public BaseCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                       final ClusterListener clusterListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        this.serverFactory = notNull("serverFactory", serverFactory);
        this.clusterListener = notNull("clusterListener", clusterListener);
        clusterListener.clusterOpened(new ClusterEvent(clusterId));
    }

    @Override
    public Server selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());

        try {
            CountDownLatch currentPhase = phase.get();
            ClusterDescription curDescription = description;
            ServerSelector compositeServerSelector = getCompositeServerSelector(serverSelector);
            Server server = selectRandomServer(compositeServerSelector, curDescription);

            boolean selectionFailureLogged = false;

            long startTimeNanos = System.nanoTime();
            long endTimeNanos = startTimeNanos + getUseableTimeoutInNanoseconds();
            long curTimeNanos = startTimeNanos;

            while (true) {
                throwIfIncompatible(curDescription);

                if (server != null) {
                    return server;
                }

                if (curTimeNanos > endTimeNanos) {
                    throw createTimeoutException(serverSelector, curDescription);
                }

                if (!selectionFailureLogged) {
                    logServerSelectionFailure(serverSelector, curDescription);
                    selectionFailureLogged = true;
                }

                connect();

                currentPhase.await(Math.min(endTimeNanos - curTimeNanos, getMinWaitTimeNanos()), NANOSECONDS);

                curTimeNanos = System.nanoTime();

                currentPhase = phase.get();
                curDescription = description;
                server = selectRandomServer(compositeServerSelector, curDescription);
            }

        } catch (InterruptedException e) {
            throw new MongoInterruptedException(format("Interrupted while waiting for a server that matches %s", serverSelector), e);
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<Server> callback) {
        isTrue("open", !isClosed());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Asynchronously selecting server with selector %s", serverSelector));
        }
        ServerSelectionRequest request = new ServerSelectionRequest(serverSelector, getCompositeServerSelector(serverSelector),
                                                                    getUseableTimeoutInNanoseconds(), callback);

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
            long endTimeNanos = startTimeNanos + getUseableTimeoutInNanoseconds();
            long curTimeNanos = startTimeNanos;

            while (curDescription.getType() == ClusterType.UNKNOWN) {

                if (curTimeNanos > endTimeNanos) {
                    throw new MongoTimeoutException(format("Timed out after %d ms while waiting to connect. Client view of cluster state "
                                                           + "is %s",
                                                           settings.getServerSelectionTimeout(MILLISECONDS),
                                                           curDescription.getShortDescription()));
                }

                if (!selectionFailureLogged) {
                    if (LOGGER.isInfoEnabled()) {
                        if (settings.getServerSelectionTimeout(MILLISECONDS) < 0) {
                            LOGGER.info(format("Cluster description not yet available. Waiting indefinitely."));
                        } else {
                            LOGGER.info(format("Cluster description not yet available. Waiting for %d ms before timing out",
                                               settings.getServerSelectionTimeout(MILLISECONDS)));
                        }
                    }
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
            stopWaitQueueHandler();
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Updating cluster description to  %s", newDescription.getShortDescription()));
        }

        description = newDescription;
        CountDownLatch current = phase.getAndSet(new CountDownLatch(1));
        current.countDown();
    }

    protected void fireChangeEvent() {
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, description));
    }

    ClusterDescription getCurrentDescription() {
        return description;
    }

    private long getUseableTimeoutInNanoseconds() {
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
                        LOGGER.trace(String.format("Asynchronously failed server selection due to driver incompatibility with server"));
                    }
                    request.onResult(null, createIncompatibleException(description));
                    return true;
                }

                Server server = selectRandomServer(request.compositeSelector, description);
                if (server != null) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(String.format("Asynchronously selected server %s", server.getDescription().getAddress()));
                    }
                    request.onResult(server, null);
                    return true;
                }
                if (prevPhase == null) {
                    logServerSelectionFailure(request.originalSelector, description);
                }
            }

            if (request.timedOut()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format("Asynchronously failed server selection after timeout"));
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


    private Server selectRandomServer(final ServerSelector serverSelector, final ClusterDescription clusterDescription) {
        List<ServerDescription> serverDescriptions = serverSelector.select(clusterDescription);
        if (!serverDescriptions.isEmpty()) {
            return getRandomServer(new ArrayList<ServerDescription>(serverDescriptions));
        } else {
            return null;
        }
    }

    private ServerSelector getCompositeServerSelector(final ServerSelector serverSelector) {
        if (settings.getServerSelector() == null) {
            return serverSelector;
        } else {
            return new CompositeServerSelector(asList(serverSelector, settings.getServerSelector()));
        }
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
            throw createIncompatibleException(curDescription);
        }
    }

    private MongoIncompatibleDriverException createIncompatibleException(final ClusterDescription curDescription) {
        return new MongoIncompatibleDriverException(format("This version of the driver is not compatible with one or more of "
                                                           + "the servers to which it is connected: %s", curDescription),
                                                    curDescription);
    }

    private MongoTimeoutException createTimeoutException(final ServerSelector serverSelector, final ClusterDescription curDescription) {
        return new MongoTimeoutException(format("Timed out after %d ms while waiting for a server that matches %s. "
                                                + "Client view of cluster state is %s",
                                                settings.getServerSelectionTimeout(MILLISECONDS), serverSelector,
                                                curDescription.getShortDescription()));
    }

    private MongoWaitQueueFullException createWaitQueueFullException() {
        return new MongoWaitQueueFullException(format("Too many operations are already waiting for a server. "
                                                      + "Max number of operations (maxWaitQueueSize) of %d has "
                                                      + "been exceeded.",
                                                      settings.getMaxWaitQueueSize()));
    }

    private static final class ServerSelectionRequest {
        private final ServerSelector originalSelector;
        private final ServerSelector compositeSelector;
        private final long maxWaitTimeNanos;
        private final SingleResultCallback<Server> callback;
        private final long startTimeNanos = System.nanoTime();
        private CountDownLatch phase;

        ServerSelectionRequest(final ServerSelector serverSelector, final ServerSelector compositeSelector,
                               final long maxWaitTimeNanos,
                               final SingleResultCallback<Server> callback) {
            this.originalSelector = serverSelector;
            this.compositeSelector = compositeSelector;
            this.maxWaitTimeNanos = maxWaitTimeNanos;
            this.callback = callback;
        }

        void onResult(final Server server, final Throwable t) {
            try {
                callback.onResult(server, t);
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

    private synchronized void notifyWaitQueueHandler(final ServerSelectionRequest request) {
        if (isClosed) {
            return;
        }

        if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
            waitQueueSize.decrementAndGet();
            request.onResult(null, createWaitQueueFullException());
        } else {
            waitQueue.add(request);

            if (waitQueueHandler == null) {
                waitQueueHandler = new Thread(new WaitQueueHandler(), "cluster-" + clusterId.getValue());
                waitQueueHandler.setDaemon(true);
                waitQueueHandler.start();
            }
        }
    }

    private synchronized void stopWaitQueueHandler() {
        if (waitQueueHandler != null) {
            waitQueueHandler.interrupt();
        }
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
                        waitQueueSize.decrementAndGet();
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

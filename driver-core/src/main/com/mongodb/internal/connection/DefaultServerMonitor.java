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

import com.mongodb.MongoSocketException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.internal.event.EventListenerHelper.getServerMonitorListener;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
class DefaultServerMonitor implements ServerMonitor {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerId serverId;
    private final ServerMonitorListener serverMonitorListener;
    private final ClusterClock clusterClock;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ConnectionPool connectionPool;
    private final ServerSettings serverSettings;
    private final ServerMonitorRunnable monitor;
    private final Thread monitorThread;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile boolean isClosed;

    DefaultServerMonitor(final ServerId serverId, final ServerSettings serverSettings,
                         final ClusterClock clusterClock, final ChangeListener<ServerDescription> serverStateListener,
                         final InternalConnectionFactory internalConnectionFactory, final ConnectionPool connectionPool) {
        this.serverSettings = notNull("serverSettings", serverSettings);
        this.serverId = notNull("serverId", serverId);
        this.serverMonitorListener = getServerMonitorListener(serverSettings);
        this.clusterClock = notNull("clusterClock", clusterClock);
        this.serverStateListener = serverStateListener;
        this.internalConnectionFactory = notNull("internalConnectionFactory", internalConnectionFactory);
        this.connectionPool = connectionPool;
        monitor = new ServerMonitorRunnable();
        monitorThread = new Thread(monitor, "cluster-" + this.serverId.getClusterId() + "-" + this.serverId.getAddress());
        monitorThread.setDaemon(true);
        isClosed = false;
    }

    @Override
    public void start() {
        monitorThread.start();
    }

    @Override
    public void connect() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        isClosed = true;
        monitorThread.interrupt();
    }

    class ServerMonitorRunnable implements Runnable {
        private final ExponentiallyWeightedMovingAverage averageRoundTripTime = new ExponentiallyWeightedMovingAverage(0.2);

        @Override
        @SuppressWarnings("unchecked")
        public synchronized void run() {
            InternalConnection connection = null;
            try {
                ServerDescription currentServerDescription = getConnectingServerDescription(null);
                while (!isClosed) {
                    ServerDescription previousServerDescription = currentServerDescription;
                    try {
                        if (connection == null) {
                            connection = internalConnectionFactory.create(serverId);
                            try {
                                connection.open();
                                currentServerDescription = connection.getInitialServerDescription();
                            } catch (Throwable t) {
                                connection = null;
                                throw t;
                            }
                        } else {
                            try {
                                currentServerDescription = lookupServerDescription(connection);
                            } catch (MongoSocketException e) {
                                connectionPool.invalidate();
                                connection.close();
                                connection = null;
                                connection = internalConnectionFactory.create(serverId);
                                try {
                                    connection.open();
                                    currentServerDescription = connection.getInitialServerDescription();
                                } catch (Throwable t) {
                                    connection = null;
                                    throw t;
                                }
                            }
                        }
                    } catch (Throwable t) {
                        averageRoundTripTime.reset();
                        currentServerDescription = getConnectingServerDescription(t);
                    }

                    if (!isClosed) {
                        try {
                            logStateChange(previousServerDescription, currentServerDescription);
                            serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(previousServerDescription,
                                    currentServerDescription));
                        } catch (Throwable t) {
                            LOGGER.warn("Exception in monitor thread during notification of server description state change", t);
                        }
                        waitForNext();
                    }
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }

        private ServerDescription getConnectingServerDescription(final Throwable exception) {
            return ServerDescription.builder().type(UNKNOWN).state(CONNECTING).address(serverId.getAddress()).exception(exception).build();
        }

        private ServerDescription lookupServerDescription(final InternalConnection connection) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Checking status of %s", serverId.getAddress()));
            }
            serverMonitorListener.serverHearbeatStarted(new ServerHeartbeatStartedEvent(connection.getDescription().getConnectionId()));

            long start = System.nanoTime();
            try {
                BsonDocument isMasterResult =
                        executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), clusterClock, connection);
                long elapsedTimeNanos = System.nanoTime() - start;
                averageRoundTripTime.addSample(elapsedTimeNanos);

                serverMonitorListener.serverHeartbeatSucceeded(
                        new ServerHeartbeatSucceededEvent(connection.getDescription().getConnectionId(), isMasterResult, elapsedTimeNanos));

                return createServerDescription(serverId.getAddress(), isMasterResult, averageRoundTripTime.getAverage());
            } catch (RuntimeException e) {
                serverMonitorListener.serverHeartbeatFailed(
                        new ServerHeartbeatFailedEvent(connection.getDescription().getConnectionId(), System.nanoTime() - start, e));
                throw e;
            }
        }

        private void logStateChange(final ServerDescription previousServerDescription,
                                    final ServerDescription currentServerDescription) {
            if (shouldLogStageChange(previousServerDescription, currentServerDescription)) {
                if (currentServerDescription.getException() != null) {
                    LOGGER.info(format("Exception in monitor thread while connecting to server %s", serverId.getAddress()),
                                currentServerDescription.getException());
                } else {
                    LOGGER.info(format("Monitor thread successfully connected to server with description %s", currentServerDescription));
                }
            }
        }

        private void waitForNext() {
            try {
                long timeRemaining = waitForSignalOrTimeout();
                if (timeRemaining > 0) {
                    long timeWaiting = serverSettings.getHeartbeatFrequency(NANOSECONDS) - timeRemaining;
                    long minimumNanosToWait = serverSettings.getMinHeartbeatFrequency(NANOSECONDS);
                    if (timeWaiting < minimumNanosToWait) {
                        long millisToSleep = MILLISECONDS.convert(minimumNanosToWait - timeWaiting, NANOSECONDS);
                        if (millisToSleep > 0) {
                            Thread.sleep(millisToSleep);
                        }
                    }
                }
            } catch (InterruptedException e) {
                // fall through
            }
        }

        private long waitForSignalOrTimeout() throws InterruptedException {
            lock.lock();
            try {
                return condition.awaitNanos(serverSettings.getHeartbeatFrequency(NANOSECONDS));
            } finally {
                lock.unlock();
            }
        }
    }

    static boolean shouldLogStageChange(final ServerDescription previous, final ServerDescription current) {

        if (previous.isOk() != current.isOk()) {
            return true;
        }
        if (!previous.getAddress().equals(current.getAddress())) {
            return true;
        }
        if (previous.getCanonicalAddress() != null
                    ? !previous.getCanonicalAddress().equals(current.getCanonicalAddress()) : current.getCanonicalAddress() != null) {
            return true;
        }
        if (!previous.getHosts().equals(current.getHosts())) {
            return true;
        }
        if (!previous.getArbiters().equals(current.getArbiters())) {
            return true;
        }
        if (!previous.getPassives().equals(current.getPassives())) {
            return true;
        }
        if (previous.getPrimary() != null ? !previous.getPrimary().equals(current.getPrimary()) : current.getPrimary() != null) {
            return true;
        }
        if (previous.getSetName() != null ? !previous.getSetName().equals(current.getSetName()) : current.getSetName() != null) {
            return true;
        }
        if (previous.getState() != current.getState()) {
            return true;
        }
        if (!previous.getTagSet().equals(current.getTagSet())) {
            return true;
        }
        if (previous.getType() != current.getType()) {
            return true;
        }
        if (previous.getMaxWireVersion() != current.getMaxWireVersion()) {
            return true;
        }
        ObjectId previousElectionId = previous.getElectionId();
        if (previousElectionId != null
                    ? !previousElectionId.equals(current.getElectionId()) : current.getElectionId() != null) {
            return true;
        }
        Integer setVersion = previous.getSetVersion();
        if (setVersion != null
                    ? !setVersion.equals(current.getSetVersion()) : current.getSetVersion() != null) {
            return true;
        }

        // Compare class equality and message as exceptions rarely override equals
        Throwable previousException = previous.getException();
        Throwable currentException = current.getException();
        Class<?> thisExceptionClass = previousException != null ? previousException.getClass() : null;
        Class<?> thatExceptionClass = currentException != null ? currentException.getClass() : null;
        if (thisExceptionClass != null ? !thisExceptionClass.equals(thatExceptionClass) : thatExceptionClass != null) {
            return true;
        }

        String thisExceptionMessage = previousException != null ? previousException.getMessage() : null;
        String thatExceptionMessage = currentException != null ? currentException.getMessage() : null;
        if (thisExceptionMessage != null ? !thisExceptionMessage.equals(thatExceptionMessage) : thatExceptionMessage != null) {
            return true;
        }

        return false;
    }
}

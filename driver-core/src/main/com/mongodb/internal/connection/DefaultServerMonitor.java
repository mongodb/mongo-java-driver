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

import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerApi;
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
import com.mongodb.internal.inject.Provider;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.ObjectId;

import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static com.mongodb.internal.connection.CommandHelper.HELLO;
import static com.mongodb.internal.connection.CommandHelper.LEGACY_HELLO;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.internal.connection.ServerDescriptionHelper.unknownConnectingServerDescription;
import static com.mongodb.internal.event.EventListenerHelper.singleServerMonitorListener;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
class DefaultServerMonitor implements ServerMonitor {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerId serverId;
    private final ServerMonitorListener serverMonitorListener;
    private final ClusterClock clusterClock;
    private final Provider<SdamServerDescriptionManager> sdamProvider;
    private final InternalConnectionFactory internalConnectionFactory;
    @Nullable
    private final ServerApi serverApi;
    private final ServerSettings serverSettings;
    private final ServerMonitorRunnable monitor;
    private final Thread monitorThread;
    private final RoundTripTimeRunnable roundTripTimeMonitor;
    private final ExponentiallyWeightedMovingAverage averageRoundTripTime = new ExponentiallyWeightedMovingAverage(0.2);
    private final Thread roundTripTimeMonitorThread;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile boolean isClosed;

    DefaultServerMonitor(final ServerId serverId, final ServerSettings serverSettings,
                         final ClusterClock clusterClock,
                         final InternalConnectionFactory internalConnectionFactory, final @Nullable ServerApi serverApi,
                         final Provider<SdamServerDescriptionManager> sdamProvider) {
        this.serverSettings = notNull("serverSettings", serverSettings);
        this.serverId = notNull("serverId", serverId);
        this.serverMonitorListener = singleServerMonitorListener(serverSettings);
        this.clusterClock = notNull("clusterClock", clusterClock);
        this.internalConnectionFactory = notNull("internalConnectionFactory", internalConnectionFactory);
        this.serverApi = serverApi;
        this.sdamProvider = sdamProvider;
        monitor = new ServerMonitorRunnable();
        monitorThread = new Thread(monitor, "cluster-" + this.serverId.getClusterId() + "-" + this.serverId.getAddress());
        monitorThread.setDaemon(true);
        roundTripTimeMonitor = new RoundTripTimeRunnable();
        roundTripTimeMonitorThread = new Thread(roundTripTimeMonitor,
                "cluster-rtt-" + this.serverId.getClusterId() + "-" + this.serverId.getAddress());
        roundTripTimeMonitorThread.setDaemon(true);
        isClosed = false;
    }

    @Override
    public void start() {
        monitorThread.start();
        roundTripTimeMonitorThread.start();
    }

    @Override
    public void connect() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        isClosed = true;
        monitor.close();
        monitorThread.interrupt();
        roundTripTimeMonitor.close();
        roundTripTimeMonitorThread.interrupt();
    }

    @Override
    public void cancelCurrentCheck() {
        monitor.cancelCurrentCheck();
    }

    class ServerMonitorRunnable implements Runnable {
        private volatile InternalConnection connection = null;
        private volatile boolean currentCheckCancelled;

        void close() {
            InternalConnection connection = this.connection;
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void run() {
            ServerDescription currentServerDescription = unknownConnectingServerDescription(serverId, null);
            try {
                while (!isClosed) {
                    ServerDescription previousServerDescription = currentServerDescription;
                    currentServerDescription = lookupServerDescription(currentServerDescription);

                    if (isClosed) {
                        continue;
                    }

                    if (currentCheckCancelled) {
                        waitForNext();
                        currentCheckCancelled = false;
                        continue;
                    }

                    logStateChange(previousServerDescription, currentServerDescription);
                    sdamProvider.get().update(currentServerDescription);

                    if (((connection == null || shouldStreamResponses(currentServerDescription))
                            && currentServerDescription.getTopologyVersion() != null)
                            || (connection != null && connection.hasMoreToCome())
                            || (currentServerDescription.getException() instanceof MongoSocketException
                            && previousServerDescription.getType() != UNKNOWN)) {
                        continue;
                    }
                    waitForNext();
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }

        private ServerDescription lookupServerDescription(final ServerDescription currentServerDescription) {
            try {
                if (connection == null || connection.isClosed()) {
                    currentCheckCancelled = false;
                    InternalConnection newConnection = internalConnectionFactory.create(serverId);
                    newConnection.open();
                    connection = newConnection;
                    averageRoundTripTime.addSample(connection.getInitialServerDescription().getRoundTripTimeNanos());
                    return connection.getInitialServerDescription();
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(format("Checking status of %s", serverId.getAddress()));
                }
                serverMonitorListener.serverHearbeatStarted(new ServerHeartbeatStartedEvent(connection.getDescription().getConnectionId()));

                long start = System.nanoTime();
                try {
                    SessionContext sessionContext = new ClusterClockAdvancingSessionContext(NoOpSessionContext.INSTANCE, clusterClock);
                    if (!connection.hasMoreToCome()) {
                        BsonDocument helloDocument = new BsonDocument(getHandshakeCommandName(currentServerDescription), new BsonInt32(1))
                                .append("helloOk", BsonBoolean.TRUE);
                        if (shouldStreamResponses(currentServerDescription)) {
                            helloDocument.append("topologyVersion", currentServerDescription.getTopologyVersion().asDocument());
                            helloDocument.append("maxAwaitTimeMS", new BsonInt64(serverSettings.getHeartbeatFrequency(MILLISECONDS)));
                        }

                        connection.send(createCommandMessage(helloDocument, connection, currentServerDescription), new BsonDocumentCodec(),
                                sessionContext);
                    }

                    BsonDocument helloResult;
                    if (shouldStreamResponses(currentServerDescription)) {
                        helloResult = connection.receive(new BsonDocumentCodec(), sessionContext,
                                Math.toIntExact(serverSettings.getHeartbeatFrequency(MILLISECONDS)));
                    } else {
                        helloResult = connection.receive(new BsonDocumentCodec(), sessionContext);
                    }

                    long elapsedTimeNanos = System.nanoTime() - start;
                    serverMonitorListener.serverHeartbeatSucceeded(
                            new ServerHeartbeatSucceededEvent(connection.getDescription().getConnectionId(), helloResult,
                                    elapsedTimeNanos, currentServerDescription.getTopologyVersion() != null));

                    return createServerDescription(serverId.getAddress(), helloResult, averageRoundTripTime.getAverage());
                } catch (RuntimeException e) {
                    serverMonitorListener.serverHeartbeatFailed(
                            new ServerHeartbeatFailedEvent(connection.getDescription().getConnectionId(), System.nanoTime() - start,
                                    currentServerDescription.getTopologyVersion() != null, e));
                    throw e;
                }
            } catch (Throwable t) {
                averageRoundTripTime.reset();
                InternalConnection localConnection;
                synchronized (this) {
                    localConnection = connection;
                    connection = null;
                }
                if (localConnection != null) {
                    localConnection.close();
                }
                return unknownConnectingServerDescription(serverId, t);
            }
        }

        private boolean shouldStreamResponses(final ServerDescription currentServerDescription) {
            return currentServerDescription.getTopologyVersion() != null && connection.supportsAdditionalTimeout();
        }

        private CommandMessage createCommandMessage(final BsonDocument command, final InternalConnection connection,
                                                    final ServerDescription currentServerDescription) {
            return new CommandMessage(new MongoNamespace("admin", COMMAND_COLLECTION_NAME), command,
                    new NoOpFieldNameValidator(), primary(),
                    MessageSettings.builder()
                            .maxWireVersion(connection.getDescription().getMaxWireVersion())
                            .build(),
                    shouldStreamResponses(currentServerDescription), serverApi);
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

        public void cancelCurrentCheck() {
            InternalConnection localConnection = null;
            synchronized (this) {
                if (connection != null && !currentCheckCancelled) {
                    localConnection = connection;
                    currentCheckCancelled = true;
                }
            }
            if (localConnection != null) {
                localConnection.close();
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
        if (!Objects.equals(thisExceptionClass, thatExceptionClass)) {
            return true;
        }

        String thisExceptionMessage = previousException != null ? previousException.getMessage() : null;
        String thatExceptionMessage = currentException != null ? currentException.getMessage() : null;
        if (!Objects.equals(thisExceptionMessage, thatExceptionMessage)) {
            return true;
        }

        return false;
    }


    private class RoundTripTimeRunnable implements Runnable {
        private volatile InternalConnection connection = null;

        void close() {
            InternalConnection connection = this.connection;
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void run() {
            try {
                while (!isClosed) {
                    try {
                        if (connection == null) {
                            initialize();
                        } else {
                            pingServer(connection);
                        }
                    } catch (Throwable t) {
                        if (connection != null) {
                            connection.close();
                            connection = null;
                        }
                    }
                    waitForNext();
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }

        private void initialize() {
            connection = null;
            connection = internalConnectionFactory.create(serverId);
            connection.open();
            averageRoundTripTime.addSample(connection.getInitialServerDescription().getRoundTripTimeNanos());
        }

        private void pingServer(final InternalConnection connection) {
            long start = System.nanoTime();
            executeCommand("admin",
                    new BsonDocument(getHandshakeCommandName(connection.getInitialServerDescription()), new BsonInt32(1)),
                    clusterClock, serverApi, connection);
            long elapsedTimeNanos = System.nanoTime() - start;
            averageRoundTripTime.addSample(elapsedTimeNanos);
        }
    }

    private void waitForNext() {
        try {
            Thread.sleep(serverSettings.getHeartbeatFrequency(MILLISECONDS));
        } catch (InterruptedException e) {
            // fall through
        }
    }

    private String getHandshakeCommandName(final ServerDescription serverDescription) {
        return serverDescription.isHelloOk() ? HELLO : LEGACY_HELLO;
    }
}

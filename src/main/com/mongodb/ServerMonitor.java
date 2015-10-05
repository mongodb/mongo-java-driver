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

import org.bson.util.annotations.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
@SuppressWarnings("deprecation")
class ServerMonitor {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerAddress serverAddress;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final SocketSettings socketSettings;
    private final ServerSettings settings;
    private final Mongo mongo;
    private final PooledConnectionProvider connectionProvider;
    private int count;
    private long elapsedNanosSum;
    private volatile boolean isClosed;
    private final Thread monitorThread;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();


    ServerMonitor(final ServerAddress serverAddress, final ChangeListener<ServerDescription> serverStateListener,
                  final SocketSettings socketSettings, final ServerSettings settings, final String clusterId, Mongo mongo,
                  final PooledConnectionProvider connectionProvider) {
        this.serverAddress = serverAddress;
        this.serverStateListener = serverStateListener;
        this.socketSettings = socketSettings;
        this.settings = settings;
        this.mongo = mongo;
        this.connectionProvider = connectionProvider;
        monitorThread = new Thread(new ServerMonitorRunnable(), "cluster-" + clusterId + "-" + serverAddress);
        monitorThread.setDaemon(true);
    }

    void start() {
        monitorThread.start();
    }

    class ServerMonitorRunnable implements Runnable {
        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            DBPort connection = null;
            try {
                ServerDescription currentServerDescription = getConnectingServerDescription(null);
                Throwable currentException = null;
                while (!isClosed) {
                    ServerDescription previousServerDescription = currentServerDescription;
                    Throwable previousException = currentException;
                    try {
                        if (connection == null) {
                            connection = new DBPort(serverAddress, mongo, getOptions());
                        }
                        try {
                            currentServerDescription = lookupServerDescription(connection);
                        } catch (IOException e) {
                            // in case the connection has been reset since the last run, do one retry immediately before reporting that the
                            // server is down
                            count = 0;
                            elapsedNanosSum = 0;
                            if (connection != null) {
                                connection.close();
                                connection = null;
                                connectionProvider.invalidate();
                            }
                            connection = new DBPort(serverAddress, mongo, getOptions());
                            try {
                                currentServerDescription = lookupServerDescription(connection);
                            } catch (IOException e1) {
                                connection.close();
                                connection = null;
                                throw e1;
                            }
                        }
                    } catch (Throwable t) {
                        currentException = t;
                        currentServerDescription = getConnectingServerDescription(t);
                    }

                    if (!isClosed) {
                        try {
                            logStateChange(previousServerDescription, previousException, currentServerDescription, currentException);
                            sendStateChangedEvent(previousServerDescription, currentServerDescription);
                        } catch (Throwable t) {
                            LOGGER.log(Level.WARNING, "Exception in monitor thread during notification of server state change", t);
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

        private void sendStateChangedEvent(final ServerDescription previousServerDescription,
                                           final ServerDescription currentServerDescription) {
            if (stateHasChanged(previousServerDescription, currentServerDescription)) {
                serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(previousServerDescription,
                                                                                    currentServerDescription));
            }
        }

        private void logStateChange(final ServerDescription previousServerDescription, final Throwable previousException,
                                    final ServerDescription currentServerDescription, final Throwable currentException) {
            // Note that the ServerDescription.equals method does not include the average ping time as part of the comparison,
            // so this will not spam the logs too hard.
            if (descriptionHasChanged(previousServerDescription, currentServerDescription)
                || exceptionHasChanged(previousException, currentException)) {
                if (currentException != null) {
                    LOGGER.log(Level.INFO, format("Exception in monitor thread while connecting to server %s", serverAddress),
                               currentException);
                } else {
                    LOGGER.info(format("Monitor thread successfully connected to server with description %s", currentServerDescription));
                }
            }
        }

        private void waitForNext() {
            try {
                long timeRemaining = waitForSignalOrTimeout();
                if (timeRemaining > 0) {
                    long timeWaiting = settings.getHeartbeatFrequency(NANOSECONDS) - timeRemaining;
                    long minimumNanosToWait = settings.getHeartbeatConnectRetryFrequency(NANOSECONDS);
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
                return condition.awaitNanos(settings.getHeartbeatFrequency(NANOSECONDS));
            } finally {
                lock.unlock();
            }
        }
    }

    public void connect() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        isClosed = true;
        monitorThread.interrupt();
    }

    private MongoOptions getOptions() {
        MongoOptions options = new MongoOptions();
        options.setConnectTimeout(socketSettings.getConnectTimeout(MILLISECONDS));
        options.setSocketTimeout(socketSettings.getReadTimeout(MILLISECONDS));
        options.setSocketFactory(socketSettings.getSocketFactory());
        return options;
    }

    static boolean descriptionHasChanged(final ServerDescription previousServerDescription,
                                         final ServerDescription currentServerDescription) {
        return !previousServerDescription.equals(currentServerDescription);
    }

    static boolean stateHasChanged(final ServerDescription previousServerDescription, final ServerDescription currentServerDescription) {
        return descriptionHasChanged(previousServerDescription, currentServerDescription) ||
               previousServerDescription.getAverageLatencyNanos() != currentServerDescription.getAverageLatencyNanos();
    }

    static boolean exceptionHasChanged(final Throwable previousException, final Throwable currentException) {
        if (currentException == null) {
            return previousException != null;
        } else if (previousException == null) {
            return true;
        } else if (!currentException.getClass().equals(previousException.getClass())) {
            return true;
        } else if (currentException.getMessage() == null) {
            return previousException.getMessage() != null;
        } else {
            return !currentException.getMessage().equals(previousException.getMessage());
        }
    }

    private ServerDescription lookupServerDescription(final DBPort connection) throws IOException {
        LOGGER.fine(format("Checking status of %s", serverAddress));
        long startNanoTime = System.nanoTime();
        final CommandResult isMasterResult = connection.runCommand(mongo.getDB("admin"), new BasicDBObject("ismaster", 1));
        count++;
        elapsedNanosSum += System.nanoTime() - startNanoTime;

        return createDescription(isMasterResult, connection.getServerVersion(), elapsedNanosSum / count);
    }

    @SuppressWarnings("unchecked")
    private ServerDescription createDescription(final CommandResult commandResult, final ServerVersion serverVersion,
                                                final long averageLatencyNanos) {
        return ServerDescription.builder()
                                .state(ServerConnectionState.Connected)
                                .version(serverVersion)
                                .address(commandResult.getServerUsed())
                                .type(getServerType(commandResult))
                                .canonicalAddress(commandResult.getString("me"))
                                .hosts(listToSet((List<String>) commandResult.get("hosts")))
                                .passives(listToSet((List<String>) commandResult.get("passives")))
                                .arbiters(listToSet((List<String>) commandResult.get("arbiters")))
                                .primary(commandResult.getString("primary"))
                                .maxDocumentSize(commandResult.getInt("maxBsonObjectSize", ServerDescription.getDefaultMaxDocumentSize()))
                                .maxMessageSize(commandResult.getInt("maxMessageSizeBytes", ServerDescription.getDefaultMaxMessageSize()))
                                .maxWriteBatchSize(commandResult.getInt("maxWriteBatchSize",
                                                                        ServerDescription.getDefaultMaxWriteBatchSize()))
                                .tagSet(getTagSetFromDocument((DBObject) commandResult.get("tags")))
                                .setName(commandResult.getString("setName"))
                                .minWireVersion(commandResult.getInt("minWireVersion", ServerDescription.getDefaultMinWireVersion()))
                                .maxWireVersion(commandResult.getInt("maxWireVersion", ServerDescription.getDefaultMaxWireVersion()))
                                .electionId(commandResult.containsKey("electionId") ? commandResult.getObjectId("electionId") : null)
                                .averageLatency(averageLatencyNanos, TimeUnit.NANOSECONDS)
                                .ok(commandResult.ok()).build();
    }

    private Set<String> listToSet(final List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptySet();
        } else {
            return new HashSet<String>(list);
        }
    }

    private static ServerType getServerType(final BasicDBObject isMasterResult) {
        if (isReplicaSetMember(isMasterResult)) {

            if (isMasterResult.getBoolean("hidden", false)) {
                return ServerType.ReplicaSetOther;
            }

            if (isMasterResult.getBoolean("ismaster", false)) {
                return ServerType.ReplicaSetPrimary;
            }

            if (isMasterResult.getBoolean("secondary", false)) {
                return ServerType.ReplicaSetSecondary;
            }

            if (isMasterResult.getBoolean("arbiterOnly", false)) {
                return ServerType.ReplicaSetArbiter;
            }

            if (isMasterResult.containsKey("setName") && isMasterResult.containsField("hosts")) {
                return ServerType.ReplicaSetOther;
            }

            return ServerType.ReplicaSetGhost;
        }

        if (isMasterResult.containsKey("msg") && isMasterResult.get("msg").equals("isdbgrid")) {
            return ServerType.ShardRouter;
        }

        return ServerType.StandAlone;
    }

    private static boolean isReplicaSetMember(final BasicDBObject isMasterResult) {
        return isMasterResult.containsKey("setName") || isMasterResult.getBoolean("isreplicaset", false);
    }

    private static TagSet getTagSetFromDocument(final DBObject tagsDocument) {
        if (tagsDocument == null) {
            return new TagSet();
        }
        List<Tag> tagList = new ArrayList<Tag>();
        for (final String key : tagsDocument.keySet()) {
            tagList.add(new Tag(key, tagsDocument.get(key).toString()));
        }
        return new TagSet(tagList);
    }

    private ServerDescription getConnectingServerDescription(final Throwable throwable) {
        return ServerDescription.builder().type(ServerType.Unknown)
                                .state(ServerConnectionState.Connecting)
                                .address(serverAddress)
                                .exception(throwable)
                                .build();
    }
}

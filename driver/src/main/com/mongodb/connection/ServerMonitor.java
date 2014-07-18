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

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.Tags;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.mongodb.CommandResult;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static com.mongodb.connection.ServerDescription.getDefaultMaxMessageSize;
import static com.mongodb.connection.ServerDescription.getDefaultMaxWireVersion;
import static com.mongodb.connection.ServerDescription.getDefaultMaxWriteBatchSize;
import static com.mongodb.connection.ServerDescription.getDefaultMinWireVersion;
import static com.mongodb.connection.ServerType.REPLICA_SET_ARBITER;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
class ServerMonitor {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerAddress serverAddress;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ConnectionPool connectionPool;
    private final ServerSettings settings;
    private final Thread monitorThread;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private int count;
    private long roundTripTimeSum;
    private volatile boolean isClosed;

    ServerMonitor(final ServerAddress serverAddress, final ServerSettings settings,
                  final String clusterId, final ChangeListener<ServerDescription> serverStateListener,
                  final InternalConnectionFactory internalConnectionFactory, final ConnectionPool connectionPool) {
        this.settings = settings;
        this.serverAddress = serverAddress;
        this.serverStateListener = serverStateListener;
        this.internalConnectionFactory = internalConnectionFactory;
        this.connectionPool = connectionPool;
        monitorThread = new Thread(new ServerMonitorRunnable(), "cluster-" + clusterId + "-" + serverAddress);
        monitorThread.setDaemon(true);
    }

    void start() {
        monitorThread.start();
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

    class ServerMonitorRunnable implements Runnable {
        @Override
        @SuppressWarnings("unchecked")
        public synchronized void run() {
            InternalConnection connection = null;
            try {
                ServerDescription currentServerDescription = getConnectingServerDescription();
                Throwable currentException = null;
                while (!isClosed) {
                    ServerDescription previousServerDescription = currentServerDescription;
                    Throwable previousException = currentException;
                    try {
                        if (connection == null) {
                            connection = internalConnectionFactory.create(serverAddress);
                        }
                        try {
                            currentServerDescription = lookupServerDescription(connection);
                        } catch (MongoSocketException e) {
                            reset();
                            connection.close();
                            connection = null;
                            connection = internalConnectionFactory.create(serverAddress);
                            try {
                                currentServerDescription = lookupServerDescription(connection);
                            } catch (MongoSocketException e1) {
                                connection.close();
                                connection = null;
                                throw e1;
                            }
                        }
                    } catch (Throwable t) {
                        currentException = t;
                        currentServerDescription = getConnectingServerDescription();
                    }

                    if (!isClosed) {
                        try {
                            logStateChange(previousServerDescription, previousException, currentServerDescription, currentException);
                            sendStateChangedEvent(previousServerDescription, currentServerDescription);
                        } catch (Throwable t) {
                            LOGGER.warn("Exception in monitor thread during notification of server description state change", t);
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
                    LOGGER.info(format("Exception in monitor thread while connecting to server %s", serverAddress), currentException);
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

    private void reset() {
        count = 0;
        roundTripTimeSum = 0;
        connectionPool.invalidate();
    }

    static boolean descriptionHasChanged(final ServerDescription previousServerDescription,
                                         final ServerDescription currentServerDescription) {
        return !previousServerDescription.equals(currentServerDescription);
    }

    static boolean stateHasChanged(final ServerDescription previousServerDescription, final ServerDescription currentServerDescription) {
        return descriptionHasChanged(previousServerDescription, currentServerDescription)
               || previousServerDescription.getRoundTripTimeNanos() != currentServerDescription.getRoundTripTimeNanos();
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

    private ServerDescription lookupServerDescription(final InternalConnection connection) {
        LOGGER.debug(format("Checking status of %s", serverAddress));
        long start = System.nanoTime();
        CommandResult isMasterResult = executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), connection);
        count++;
        roundTripTimeSum += System.nanoTime() - start;

        CommandResult buildInfoResult = executeCommand("admin", new BsonDocument("buildinfo", new BsonInt32(1)), connection);
        return createDescription(isMasterResult, buildInfoResult, roundTripTimeSum / count);
    }

    @SuppressWarnings("unchecked")
    private ServerDescription createDescription(final CommandResult commandResult, final CommandResult buildInfoResult,
                                                final long roundTripTime) {
        return ServerDescription.builder()
                                .state(CONNECTED)
                                .version(getVersion(buildInfoResult))
                                .address(commandResult.getAddress())
                                .type(getServerType(commandResult.getResponse()))
                                .hosts(listToSet(commandResult.getResponse().getArray("hosts", new BsonArray())))
                                .passives(listToSet(commandResult.getResponse().getArray("passives", new BsonArray())))
                                .arbiters(listToSet(commandResult.getResponse().getArray("arbiters", new BsonArray())))
                                .primary(getString(commandResult.getResponse(), "primary"))
                                .maxDocumentSize(commandResult.getResponse().getInt32("maxBsonObjectSize",
                                                                                      new BsonInt32(getDefaultMaxDocumentSize()))
                                                              .getValue())
                                .maxMessageSize(commandResult.getResponse().getInt32("maxMessageSizeBytes",
                                                                                     new BsonInt32(getDefaultMaxMessageSize()))
                                                             .getValue())
                                .maxWriteBatchSize(commandResult.getResponse().getInt32("maxWriteBatchSize",
                                                                                        new BsonInt32(getDefaultMaxWriteBatchSize()))
                                                                .getValue())
                                .tags(getTagsFromDocument(commandResult.getResponse().getDocument("tags", new BsonDocument())))
                                .setName(getString(commandResult.getResponse(), "setName"))
                                .minWireVersion(commandResult.getResponse().getInt32("minWireVersion",
                                                                                     new BsonInt32(getDefaultMinWireVersion())).getValue())
                                .maxWireVersion(commandResult.getResponse().getInt32("maxWireVersion",
                                                                                     new BsonInt32(getDefaultMaxWireVersion())).getValue())
                                .roundTripTime(roundTripTime, NANOSECONDS)
                                .ok(commandResult.isOk()).build();
    }

    private String getString(final BsonDocument response, final String key) {
        if (response.containsKey(key)) {
            return response.getString(key).getValue();
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static ServerVersion getVersion(final CommandResult buildInfoResult) {
        List<BsonValue> versionArray = buildInfoResult.getResponse().getArray("versionArray").subList(0, 3);

        return new ServerVersion(asList(versionArray.get(0).asInt32().getValue(),
                                        versionArray.get(1).asInt32().getValue(),
                                        versionArray.get(2).asInt32().getValue()));
    }

    private Set<String> listToSet(final BsonArray array) {
        if (array == null || array.isEmpty()) {
            return Collections.emptySet();
        } else {
            Set<String> set = new HashSet<String>();
            for (BsonValue value : array) {
                set.add(value.asString().getValue());
            }
            return set;
        }
    }

    private static ServerType getServerType(final BsonDocument isMasterResult) {
        if (isReplicaSetMember(isMasterResult)) {
            if (isMasterResult.getBoolean("ismaster", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_PRIMARY;
            }

            if (isMasterResult.getBoolean("secondary", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_SECONDARY;
            }

            if (isMasterResult.getBoolean("arbiterOnly", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_ARBITER;
            }

            if (isMasterResult.containsKey("setName") && isMasterResult.containsKey("hosts")) {
                return ServerType.REPLICA_SET_OTHER;
            }

            return ServerType.REPLICA_SET_GHOST;
        }

        if (isMasterResult.containsKey("msg") && isMasterResult.get("msg").equals(new BsonString("isdbgrid"))) {
            return SHARD_ROUTER;
        }

        return STANDALONE;
    }

    private static boolean isReplicaSetMember(final BsonDocument isMasterResult) {
        return isMasterResult.containsKey("setName") || isMasterResult.getBoolean("isreplicaset", BsonBoolean.FALSE).getValue();
    }

    private static Tags getTagsFromDocument(final BsonDocument tagsDocuments) {
        if (tagsDocuments == null) {
            return new Tags();
        }
        Tags tags = new Tags();
        for (final Map.Entry<String, BsonValue> curEntry : tagsDocuments.entrySet()) {
            tags.put(curEntry.getKey(), curEntry.getValue().toString());
        }
        return tags;
    }

    private ServerDescription getConnectingServerDescription() {
        return ServerDescription.builder().type(UNKNOWN).state(CONNECTING).address(serverAddress).build();
    }
}

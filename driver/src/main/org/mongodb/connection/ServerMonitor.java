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

package org.mongodb.connection;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.mongodb.CommandResult;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.mongodb.connection.CommandHelper.executeCommand;
import static org.mongodb.connection.ServerConnectionState.CONNECTED;
import static org.mongodb.connection.ServerConnectionState.CONNECTING;
import static org.mongodb.connection.ServerConnectionState.UNCONNECTED;
import static org.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static org.mongodb.connection.ServerDescription.getDefaultMaxMessageSize;
import static org.mongodb.connection.ServerDescription.getDefaultMaxWireVersion;
import static org.mongodb.connection.ServerDescription.getDefaultMaxWriteBatchSize;
import static org.mongodb.connection.ServerDescription.getDefaultMinWireVersion;
import static org.mongodb.connection.ServerType.REPLICA_SET_ARBITER;
import static org.mongodb.connection.ServerType.REPLICA_SET_OTHER;
import static org.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static org.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.mongodb.connection.ServerType.STANDALONE;
import static org.mongodb.connection.ServerType.UNKNOWN;

@ThreadSafe
class ServerMonitor {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerAddress serverAddress;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ServerSettings settings;
    private final Thread monitorThread;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private int count;
    private long elapsedNanosSum;
    private volatile InternalConnection connection;
    private volatile ServerDescription serverDescription;
    private volatile boolean isClosed;

    ServerMonitor(final ServerAddress serverAddress, final ServerSettings settings,
                  final String clusterId, final ChangeListener<ServerDescription> serverStateListener,
                  final InternalConnectionFactory internalConnectionFactory) {
        this.settings = settings;
        this.serverAddress = serverAddress;
        this.serverStateListener = serverStateListener;
        this.internalConnectionFactory = internalConnectionFactory;
        serverDescription = getConnectingServerDescription();
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
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    class ServerMonitorRunnable implements Runnable {
        @Override
        @SuppressWarnings("unchecked")
        public synchronized void run() {
            try {
                while (!isClosed) {
                    ServerDescription currentServerDescription = serverDescription;
                    Throwable throwable = null;
                    try {
                        if (connection == null) {
                            connection = internalConnectionFactory.create(serverAddress);
                        }
                        try {
                            serverDescription = lookupServerDescription();
                        } catch (MongoSocketException e) {
                            reset();
                            connection = internalConnectionFactory.create(serverAddress);
                            try {
                                serverDescription = lookupServerDescription();
                            } catch (MongoSocketException e1) {
                                reset();
                                throw e1;
                            }
                        }
                    } catch (Throwable t) {
                        throwable = t;
                        serverDescription = getUnconnectedServerDescription();
                    }

                    if (!isClosed) {
                        try {
                            logStateChange(currentServerDescription, throwable);
                            sendStateChangedEvent(currentServerDescription);
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

        private void sendStateChangedEvent(final ServerDescription currentServerDescription) {
            serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(currentServerDescription, serverDescription));
        }

        private void logStateChange(final ServerDescription currentServerDescription, final Throwable throwable) {
            // Note that the ServerDescription.equals method does not include the average ping time as part of the comparison,
            // so this will not spam the logs too hard.
            if (!currentServerDescription.equals(serverDescription)) {
                if (throwable != null) {
                    LOGGER.info(format("Exception in monitor thread while connecting to server %s", serverAddress), throwable);
                } else {
                    LOGGER.info(format("Monitor thread successfully connected to server with description %s", serverDescription));
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
        elapsedNanosSum = 0;
        if (connection != null) {
            connection.close();
            connection = null;
            // TODO: invalidate connection pool
        }
    }

    private ServerDescription lookupServerDescription() {
        LOGGER.debug(format("Checking status of %s", serverAddress));
        CommandResult isMasterResult = executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), connection);
        count++;
        elapsedNanosSum += isMasterResult.getElapsedNanoseconds();

        CommandResult buildInfoResult = executeCommand("admin", new BsonDocument("buildinfo", new BsonInt32(1)), connection);
        return createDescription(isMasterResult, buildInfoResult, elapsedNanosSum / count);
    }

    @SuppressWarnings("unchecked")
    private ServerDescription createDescription(final CommandResult commandResult, final CommandResult buildInfoResult,
                                                final long averagePingTimeNanos) {
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
                                .setVersion(getSetVersion(commandResult))
                                .minWireVersion(commandResult.getResponse().getInt32("minWireVersion",
                                                                                     new BsonInt32(getDefaultMinWireVersion())).getValue())
                                .maxWireVersion(commandResult.getResponse().getInt32("maxWireVersion",
                                                                                     new BsonInt32(getDefaultMaxWireVersion())).getValue())
                                .averagePingTime(averagePingTimeNanos, NANOSECONDS)
                                .ok(commandResult.isOk()).build();
    }

    private String getString(final BsonDocument response, final String key) {
        if (response.containsKey(key)) {
            return response.getString(key).getValue();
        } else {
            return null;
        }
    }

    private Integer getSetVersion(final CommandResult commandResult) {
        if (commandResult.getResponse().containsKey("setVersion")) {
            return ((BsonInt32) commandResult.getResponse().get("setVersion")).getValue();
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

            return REPLICA_SET_OTHER;
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

    private ServerDescription getUnconnectedServerDescription() {
        return ServerDescription.builder().type(UNKNOWN).state(UNCONNECTED).address(serverAddress).build();
    }
}

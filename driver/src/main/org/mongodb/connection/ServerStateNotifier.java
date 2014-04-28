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

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.mongodb.connection.CommandHelper.executeCommand;
import static org.mongodb.connection.ServerConnectionState.CONNECTED;
import static org.mongodb.connection.ServerConnectionState.CONNECTING;
import static org.mongodb.connection.ServerConnectionState.UNCONNECTED;
import static org.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static org.mongodb.connection.ServerDescription.getDefaultMaxMessageSize;
import static org.mongodb.connection.ServerDescription.getDefaultMaxWireVersion;
import static org.mongodb.connection.ServerDescription.getDefaultMinWireVersion;
import static org.mongodb.connection.ServerType.REPLICA_SET_ARBITER;
import static org.mongodb.connection.ServerType.REPLICA_SET_OTHER;
import static org.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static org.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.mongodb.connection.ServerType.STANDALONE;
import static org.mongodb.connection.ServerType.UNKNOWN;

@ThreadSafe
class ServerStateNotifier implements Runnable {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerAddress serverAddress;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final InternalConnectionFactory internalConnectionFactory;
    private InternalConnection internalConnection;
    private int count;
    private long elapsedNanosSum;
    private volatile ServerDescription serverDescription;
    private volatile boolean isClosed;

    ServerStateNotifier(final ServerAddress serverAddress, final ChangeListener<ServerDescription> serverStateListener,
                        final InternalConnectionFactory internalConnectionFactory) {
        this.serverAddress = serverAddress;
        this.serverStateListener = serverStateListener;
        this.internalConnectionFactory = internalConnectionFactory;
        serverDescription = getConnectingServerDescription();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void run() {
        if (isClosed) {
            return;
        }

        ServerDescription currentServerDescription = serverDescription;
        Throwable throwable = null;
        try {
            if (internalConnection == null) {
                internalConnection = internalConnectionFactory.create(serverAddress);
            }
            try {
                serverDescription = lookupServerDescription();
            } catch (MongoSocketException e) {
                reset();
                internalConnection = internalConnectionFactory.create(serverAddress);
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
                // Note that the ServerDescription.equals method does not include the average ping time as part of the comparison,
                // so this will not spam the logs too hard.
                if (!currentServerDescription.equals(serverDescription)) {
                    if (throwable != null) {
                        LOGGER.info(format("Exception in monitor thread while connecting to server %s", serverAddress), throwable);
                    } else {
                        LOGGER.info(format("Monitor thread successfully connected to server with description %s", serverDescription));
                    }
                }
                serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(currentServerDescription, serverDescription));
            } catch (Throwable t) {
                LOGGER.warn("Exception in monitor thread during notification of server description state change", t);
            }
        }
    }

    private void reset() {
        count = 0;
        elapsedNanosSum = 0;
        if (internalConnection != null) {
            internalConnection.close();
            internalConnection = null;
        }
    }

    private ServerDescription lookupServerDescription() {
        LOGGER.debug(format("Checking status of %s", serverAddress));
        CommandResult isMasterResult = executeCommand("admin", new Document("ismaster", 1), new DocumentCodec(),
                                                      internalConnection);
        count++;
        elapsedNanosSum += isMasterResult.getElapsedNanoseconds();

        CommandResult buildInfoResult = executeCommand("admin", new Document("buildinfo", 1), new DocumentCodec(),
                                                       internalConnection);
        return createDescription(isMasterResult, buildInfoResult, elapsedNanosSum / count);
    }

    public void close() {
        isClosed = true;
        if (internalConnection != null) {
            internalConnection.close();
            internalConnection = null;
        }
    }

    @SuppressWarnings("unchecked")
    private ServerDescription createDescription(final CommandResult commandResult, final CommandResult buildInfoResult,
                                                final long averagePingTimeNanos) {
        return ServerDescription.builder()
                                .state(CONNECTED)
                                .version(getVersion(buildInfoResult))
                                .address(commandResult.getAddress())
                                .type(getServerType(commandResult.getResponse()))
                                .hosts(listToSet((List<String>) commandResult.getResponse().get("hosts")))
                                .passives(listToSet((List<String>) commandResult.getResponse().get("passives")))
                                .arbiters(listToSet((List<String>) commandResult.getResponse().get("arbiters")))
                                .primary(commandResult.getResponse().getString("primary"))
                                .maxDocumentSize(getInteger(commandResult.getResponse().getInteger("maxBsonObjectSize"),
                                                            getDefaultMaxDocumentSize()))
                                .maxMessageSize(getInteger(commandResult.getResponse().getInteger("maxMessageSizeBytes"),
                                                           getDefaultMaxMessageSize()))
                                .maxWriteBatchSize(getInteger(commandResult.getResponse().getInteger("maxWriteBatchSize"),
                                                              ServerDescription.getDefaultMaxWriteBatchSize()))
                                .tags(getTagsFromDocument((Document) commandResult.getResponse().get("tags")))
                                .setName(commandResult.getResponse().getString("setName"))
                                .setVersion(commandResult.getResponse().getInteger("setVersion"))
                                .minWireVersion(getInteger(commandResult.getResponse().getInteger("minWireVersion"),
                                                           getDefaultMinWireVersion()))
                                .maxWireVersion(getInteger(commandResult.getResponse().getInteger("maxWireVersion"),
                                                           getDefaultMaxWireVersion()))
                                .averagePingTime(averagePingTimeNanos, NANOSECONDS)
                                .ok(commandResult.isOk()).build();
    }

    @SuppressWarnings("unchecked")
    private static ServerVersion getVersion(final CommandResult buildInfoResult) {
        return new ServerVersion(((List<Integer>) buildInfoResult.getResponse().get("versionArray")).subList(0, 3));
    }

    private Set<String> listToSet(final List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptySet();
        } else {
            return new HashSet<String>(list);
        }
    }

    private static ServerType getServerType(final Document isMasterResult) {
        if (isReplicaSetMember(isMasterResult)) {
            if (getBoolean(isMasterResult.getBoolean("ismaster"), false)) {
                return REPLICA_SET_PRIMARY;
            }

            if (getBoolean(isMasterResult.getBoolean("secondary"), false)) {
                return REPLICA_SET_SECONDARY;
            }

            if (getBoolean(isMasterResult.getBoolean("arbiterOnly"), false)) {
                return REPLICA_SET_ARBITER;
            }

            return REPLICA_SET_OTHER;
        }

        if (isMasterResult.containsKey("msg") && isMasterResult.get("msg").equals("isdbgrid")) {
            return SHARD_ROUTER;
        }

        return STANDALONE;
    }

    private static boolean isReplicaSetMember(final Document isMasterResult) {
        return isMasterResult.containsKey("setName") || getBoolean(isMasterResult.getBoolean("isreplicaset"), false);
    }

    private static boolean getBoolean(final Boolean value, final boolean defaultValue) {
        return value == null ? defaultValue : value;
    }


    private static int getInteger(final Integer value, final int defaultValue) {
        return (value != null) ? value : defaultValue;
    }

    private static Tags getTagsFromDocument(final Document tagsDocuments) {
        if (tagsDocuments == null) {
            return new Tags();
        }
        Tags tags = new Tags();
        for (final Map.Entry<String, Object> curEntry : tagsDocuments.entrySet()) {
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

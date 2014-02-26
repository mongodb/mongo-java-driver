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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
class ServerStateNotifier implements Runnable {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private ServerAddress serverAddress;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final SocketSettings socketSettings;
    private final Mongo mongo;
    private int count;
    private long elapsedNanosSum;
    private volatile ServerDescription serverDescription;
    private volatile boolean isClosed;
    DBPort connection;

    ServerStateNotifier(final ServerAddress serverAddress, final ChangeListener<ServerDescription> serverStateListener,
                        final SocketSettings socketSettings, final Mongo mongo) {
        this.serverAddress = serverAddress;
        this.serverStateListener = serverStateListener;
        this.socketSettings = socketSettings;
        this.mongo = mongo;
        serverDescription = getConnectingServerDescription();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void run() {
        if (isClosed) {
            return;
        }

        final ServerDescription currentServerDescription = serverDescription;
        Throwable throwable = null;
        try {
            if (connection == null) {
                connection = new DBPort(serverAddress, null, getOptions(), 0);
            }
            try {
                LOGGER.fine(format("Checking status of %s", serverAddress));
                long startNanoTime = System.nanoTime();
                final CommandResult isMasterResult = connection.runCommand(mongo.getDB("admin"), new BasicDBObject("ismaster", 1));
                count++;
                elapsedNanosSum += System.nanoTime() - startNanoTime;

                final CommandResult buildInfoResult = connection.runCommand(mongo.getDB("admin"), new BasicDBObject("buildinfo", 1));
                serverDescription = createDescription(isMasterResult, buildInfoResult, elapsedNanosSum / count);
            } catch (IOException e) {
                if (!isClosed) {
                    connection.close();
                    connection = null;
                    count = 0;
                    elapsedNanosSum = 0;
                    throw e;
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
                        LOGGER.log(Level.INFO, format("Exception in monitor thread while connecting to server %s", serverAddress),
                                   throwable);
                    }
                    else {
                        LOGGER.info(format("Monitor thread successfully connected to server with description %s", serverDescription));
                    }
                }
                serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(currentServerDescription, serverDescription));
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Exception in monitor thread during notification of server description state change", t);
            }
        }
    }

    public void close() {
        isClosed = true;
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    private MongoOptions getOptions() {
        MongoOptions options = new MongoOptions();
        options.setConnectTimeout(socketSettings.getConnectTimeout(MILLISECONDS));
        options.setSocketTimeout(socketSettings.getReadTimeout(MILLISECONDS));
        options.setSocketFactory(socketSettings.getSocketFactory());
        return options;
    }

    @SuppressWarnings("unchecked")
    private ServerDescription createDescription(final CommandResult commandResult, final CommandResult buildInfoResult,
                                                final long averagePingTimeNanos) {
        return ServerDescription.builder()
                                .state(ServerConnectionState.Connected)
                                .version(getVersion(buildInfoResult))
                                .address(commandResult.getServerUsed())
                                .type(getServerType(commandResult))
                                .hosts(listToSet((List<String>) commandResult.get("hosts")))
                                .passives(listToSet((List<String>) commandResult.get("passives")))
                                .arbiters(listToSet((List<String>) commandResult.get("arbiters")))
                                .primary(commandResult.getString("primary"))
                                .maxDocumentSize(commandResult.getInt("maxBsonObjectSize", ServerDescription.getDefaultMaxDocumentSize()))
                                .maxMessageSize(commandResult.getInt("maxMessageSizeBytes", ServerDescription.getDefaultMaxMessageSize()))
                                .maxWriteBatchSize(commandResult.getInt("maxWriteBatchSize",
                                                                        ServerDescription.getDefaultMaxWriteBatchSize()))
                                .tags(getTagsFromDocument((DBObject) commandResult.get("tags")))
                                .setName(commandResult.getString("setName"))
                                .setVersion((Integer) commandResult.get("setVersion"))
                                .minWireVersion(commandResult.getInt("minWireVersion", ServerDescription.getDefaultMinWireVersion()))
                                .maxWireVersion(commandResult.getInt("maxWireVersion", ServerDescription.getDefaultMaxWireVersion()))
                                .averagePingTime(averagePingTimeNanos, TimeUnit.NANOSECONDS)
                                .ok(commandResult.ok()).build();
    }

    @SuppressWarnings("unchecked")
    private static ServerVersion getVersion(final CommandResult buildInfoResult) {
        return new ServerVersion(((List<Integer>) buildInfoResult.get("versionArray")).subList(0, 3));
    }

    private Set<String> listToSet(final List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptySet();
        }
        else {
            return new HashSet<String>(list);
        }
    }

    private static ServerType getServerType(final BasicDBObject isMasterResult) {
        if (isReplicaSetMember(isMasterResult)) {
            if (isMasterResult.getBoolean("ismaster", false)) {
                return ServerType.ReplicaSetPrimary;
            }

            if (isMasterResult.getBoolean("secondary", false)) {
                return ServerType.ReplicaSetSecondary;
            }

            if (isMasterResult.getBoolean("arbiterOnly", false)) {
                return ServerType.ReplicaSetArbiter;
            }

            return ServerType.ReplicaSetOther;
        }

        if (isMasterResult.containsKey("msg") && isMasterResult.get("msg").equals("isdbgrid")) {
            return ServerType.ShardRouter;
        }

        return ServerType.StandAlone;
    }

    private static boolean isReplicaSetMember(final BasicDBObject isMasterResult) {
        return isMasterResult.containsKey("setName") || isMasterResult.getBoolean("isreplicaset", false);
    }

    private static Tags getTagsFromDocument(final DBObject tagsDocuments) {
        if (tagsDocuments == null) {
            return new Tags();
        }
        final Tags tags = new Tags();
        for (final String key : tagsDocuments.keySet()) {
            tags.put(key, tagsDocuments.get(key).toString());
        }
        return tags;
    }

    private ServerDescription getConnectingServerDescription() {
        return ServerDescription.builder().type(ServerType.Unknown).state(ServerConnectionState.Connecting).address(serverAddress).build();
    }

    private ServerDescription getUnconnectedServerDescription() {
        return ServerDescription.builder().type(ServerType.Unknown).state(ServerConnectionState.Unconnected).address(serverAddress).build();
    }
}
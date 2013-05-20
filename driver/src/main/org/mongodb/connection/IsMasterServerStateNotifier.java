/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.CommandOperation;
import org.mongodb.operation.CommandResult;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.mongodb.connection.ServerType.ReplicaSetArbiter;
import static org.mongodb.connection.ServerType.ReplicaSetOther;
import static org.mongodb.connection.ServerType.ReplicaSetPrimary;
import static org.mongodb.connection.ServerType.ReplicaSetSecondary;
import static org.mongodb.connection.ServerType.ShardRouter;
import static org.mongodb.connection.ServerType.StandAlone;

@ThreadSafe
class IsMasterServerStateNotifier implements ServerStateNotifier {

    private final ServerStateListener serverStateListener;
    private final ConnectionFactory connectionFactory;
    private final BufferPool<ByteBuffer> bufferPool;
    private Connection connection;
    private int count;
    private long elapsedNanosSum;

    IsMasterServerStateNotifier(final ServerStateListener serverStateListener, final ConnectionFactory connectionFactory,
                                final BufferPool<ByteBuffer> bufferPool) {
        this.serverStateListener = serverStateListener;
        this.connectionFactory = connectionFactory;
        this.bufferPool = bufferPool;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void run() {
        try {
            ServerDescription serverDescription = null;
            try {
                if (connection == null) {
                    connection = connectionFactory.create();
                }
                try {
                    final CommandResult commandResult = new CommandOperation("admin",
                            new MongoCommand(new Document("ismaster", 1)), new DocumentCodec(), bufferPool).execute(connection);
                    count++;
                    elapsedNanosSum += commandResult.getElapsedNanoseconds();

                    serverDescription = createDescription(commandResult, elapsedNanosSum / count);
                } catch (MongoSocketException e) {
                    connection.close();
                    connection = null;
                    count = 0;
                    elapsedNanosSum = 0;
                    throw e;
                }
            } catch (MongoException e) {
                serverStateListener.notify(e);
            } catch (Throwable t) {
                serverStateListener.notify(new MongoInternalException("Unexpected exception", t));
            }

            if (serverDescription != null) {
                serverStateListener.notify(serverDescription);
            }
        } catch (Throwable t) {
            // log something
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @SuppressWarnings("unchecked")
    private ServerDescription createDescription(final CommandResult commandResult, final long averagePingTime) {
        return ServerDescription.builder()
                .address(commandResult.getAddress())
                .type(getServerType(commandResult.getResponse()))
                .hosts((List<String>) commandResult.getResponse().get("hosts"))
                .passives((List<String>) commandResult.getResponse().get("passives"))
                .primary(commandResult.getResponse().getString("primary"))
                .maxDocumentSize(getInteger(commandResult.getResponse().getInteger("maxBsonObjectSize"),
                        ServerDescription.getDefaultMaxDocumentSize()))
                .maxMessageSize(getInteger(commandResult.getResponse().getInteger("maxMessageSizeBytes"),
                        ServerDescription.getDefaultMaxMessageSize()))
                .tags(getTagsFromDocument((Document) commandResult.getResponse().get("tags")))
                .setName(commandResult.getResponse().getString("setName"))
                .averagePingTime(averagePingTime)
                .ok(commandResult.isOk()).build();
    }

    private static ServerType getServerType(final Document isMasterResult) {
        if (isReplicaSetMember(isMasterResult)) {
            if (getBoolean(isMasterResult.getBoolean("ismaster"), false)) {
                return ReplicaSetPrimary;
            }

            if (getBoolean(isMasterResult.getBoolean("secondary"), false)) {
                return ReplicaSetSecondary;
            }

            if (getBoolean(isMasterResult.getBoolean("arbiterOnly"), false)) {
                return ReplicaSetArbiter;
            }

            return ReplicaSetOther;
        }

        if (isMasterResult.containsKey("msg") && isMasterResult.get("msg").equals("isdbgrid")) {
            return ShardRouter;
        }

        return StandAlone;
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
        final Tags tags = new Tags();
        for (final Map.Entry<String, Object> curEntry : tagsDocuments.entrySet()) {
            tags.put(curEntry.getKey(), curEntry.getValue().toString());
        }
        return tags;
    }
}

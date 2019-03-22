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

import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static com.mongodb.connection.ServerDescription.getDefaultMaxWireVersion;
import static com.mongodb.connection.ServerDescription.getDefaultMinWireVersion;
import static com.mongodb.connection.ServerType.REPLICA_SET_ARBITER;
import static com.mongodb.connection.ServerType.REPLICA_SET_OTHER;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DescriptionHelper {

    static ConnectionDescription createConnectionDescription(final ConnectionId connectionId,
                                                             final BsonDocument isMasterResult) {
        ConnectionDescription connectionDescription = new ConnectionDescription(connectionId,
                getMaxWireVersion(isMasterResult), getServerType(isMasterResult), getMaxWriteBatchSize(isMasterResult),
                getMaxBsonObjectSize(isMasterResult), getMaxMessageSizeBytes(isMasterResult), getCompressors(isMasterResult));
        if (isMasterResult.containsKey("connectionId")) {
            ConnectionId newConnectionId =
                    connectionDescription.getConnectionId().withServerValue(isMasterResult.getNumber("connectionId").intValue());
            connectionDescription = connectionDescription.withConnectionId(newConnectionId);
        }
        return connectionDescription;
    }

    public static ServerDescription createServerDescription(final ServerAddress serverAddress, final BsonDocument isMasterResult,
                                                            final long roundTripTime) {
        return ServerDescription.builder()
                                .state(CONNECTED)
                                .address(serverAddress)
                                .type(getServerType(isMasterResult))
                                .canonicalAddress(isMasterResult.containsKey("me") ? isMasterResult.getString("me").getValue() : null)
                                .hosts(listToSet(isMasterResult.getArray("hosts", new BsonArray())))
                                .passives(listToSet(isMasterResult.getArray("passives", new BsonArray())))
                                .arbiters(listToSet(isMasterResult.getArray("arbiters", new BsonArray())))
                                .primary(getString(isMasterResult, "primary"))
                                .maxDocumentSize(getMaxBsonObjectSize(isMasterResult))
                                .tagSet(getTagSetFromDocument(isMasterResult.getDocument("tags", new BsonDocument())))
                                .setName(getString(isMasterResult, "setName"))
                                .minWireVersion(getMinWireVersion(isMasterResult))
                                .maxWireVersion(getMaxWireVersion(isMasterResult))
                                .electionId(getElectionId(isMasterResult))
                                .setVersion(getSetVersion(isMasterResult))
                                .lastWriteDate(getLastWriteDate(isMasterResult))
                                .roundTripTime(roundTripTime, NANOSECONDS)
                                .logicalSessionTimeoutMinutes(getLogicalSessionTimeoutMinutes(isMasterResult))
                                .ok(CommandHelper.isCommandOk(isMasterResult)).build();
    }

    private static int getMinWireVersion(final BsonDocument isMasterResult) {
        return isMasterResult.getInt32("minWireVersion", new BsonInt32(getDefaultMinWireVersion())).getValue();
    }

    private static int getMaxWireVersion(final BsonDocument isMasterResult) {
        return isMasterResult.getInt32("maxWireVersion", new BsonInt32(getDefaultMaxWireVersion())).getValue();
    }

    private static Date getLastWriteDate(final BsonDocument isMasterResult) {
        if (!isMasterResult.containsKey("lastWrite")) {
            return null;
        }
        return new Date(isMasterResult.getDocument("lastWrite").getDateTime("lastWriteDate").getValue());
    }

    private static ObjectId getElectionId(final BsonDocument isMasterResult) {
        return isMasterResult.containsKey("electionId") ? isMasterResult.getObjectId("electionId").getValue() : null;
    }

    private static Integer getSetVersion(final BsonDocument isMasterResult) {
        return isMasterResult.containsKey("setVersion") ? isMasterResult.getNumber("setVersion").intValue() : null;
    }

    private static int getMaxMessageSizeBytes(final BsonDocument isMasterResult) {
        return isMasterResult.getInt32("maxMessageSizeBytes", new BsonInt32(getDefaultMaxMessageSize())).getValue();
    }

    private static int getMaxBsonObjectSize(final BsonDocument isMasterResult) {
        return isMasterResult.getInt32("maxBsonObjectSize", new BsonInt32(getDefaultMaxDocumentSize())).getValue();
    }

    private static int getMaxWriteBatchSize(final BsonDocument isMasterResult) {
        return isMasterResult.getInt32("maxWriteBatchSize", new BsonInt32(getDefaultMaxWriteBatchSize())).getValue();
    }

    private static Integer getLogicalSessionTimeoutMinutes(final BsonDocument isMasterResult) {
        return isMasterResult.isNumber("logicalSessionTimeoutMinutes")
                       ? isMasterResult.getNumber("logicalSessionTimeoutMinutes").intValue() : null;
    }

    private static String getString(final BsonDocument response, final String key) {
        if (response.containsKey(key)) {
            return response.getString(key).getValue();
        } else {
            return null;
        }
    }

    private static Set<String> listToSet(final BsonArray array) {
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

        if (!CommandHelper.isCommandOk(isMasterResult)) {
            return UNKNOWN;
        }

        if (isReplicaSetMember(isMasterResult)) {

            if (isMasterResult.getBoolean("hidden", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_OTHER;
            }

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

    private static TagSet getTagSetFromDocument(final BsonDocument tagsDocuments) {
        List<Tag> tagList = new ArrayList<Tag>();
        for (final Map.Entry<String, BsonValue> curEntry : tagsDocuments.entrySet()) {
            tagList.add(new Tag(curEntry.getKey(), curEntry.getValue().asString().getValue()));
        }
        return new TagSet(tagList);
    }

    private static List<String> getCompressors(final BsonDocument isMasterResult) {
        List<String> compressorList = new ArrayList<String>();
        for (BsonValue compressor : isMasterResult.getArray("compression", new BsonArray())) {
            compressorList.add(compressor.asString().getValue());
        }
        return compressorList;
    }

    private DescriptionHelper() {
    }
}

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

import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.TopologyVersion;
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

import static com.mongodb.assertions.Assertions.assertNotNull;
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
import static com.mongodb.internal.connection.CommandHelper.LEGACY_HELLO_LOWER;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DescriptionHelper {

    private static volatile boolean manufactureServiceId = false;

    public static void enableServiceIdManufacturing() {
        manufactureServiceId = true;
    }

    static ConnectionDescription createConnectionDescription(final ClusterConnectionMode clusterConnectionMode,
                                                             final ConnectionId connectionId, final BsonDocument helloResult) {
        ConnectionDescription connectionDescription = new ConnectionDescription(connectionId,
                getMaxWireVersion(helloResult), getServerType(helloResult), getMaxWriteBatchSize(helloResult),
                getMaxBsonObjectSize(helloResult), getMaxMessageSizeBytes(helloResult), getCompressors(helloResult),
                helloResult.getArray("saslSupportedMechs", null));
        if (helloResult.containsKey("connectionId")) {
            ConnectionId newConnectionId =
                    connectionDescription.getConnectionId().withServerValue(helloResult.getNumber("connectionId").intValue());
            connectionDescription = connectionDescription.withConnectionId(newConnectionId);
        }
        if (clusterConnectionMode == ClusterConnectionMode.LOAD_BALANCED) {
            ObjectId serviceId = getServiceId(helloResult);
            if (serviceId != null) {
                connectionDescription = connectionDescription.withServiceId(serviceId);
            } else if (manufactureServiceId) {
                TopologyVersion topologyVersion = getTopologyVersion(helloResult);
                assertNotNull(topologyVersion);
                connectionDescription = connectionDescription.withServiceId(topologyVersion.getProcessId());
            } else {
                throw new MongoClientException("Driver attempted to initialize in load balancing mode, but the server does not support "
                        + "this mode");
            }
        }
        return connectionDescription;
    }

    public static ServerDescription createServerDescription(final ServerAddress serverAddress, final BsonDocument helloResult,
                                                            final long roundTripTime) {
        return ServerDescription.builder()
                                .state(CONNECTED)
                                .address(serverAddress)
                                .type(getServerType(helloResult))
                                .canonicalAddress(helloResult.containsKey("me") ? helloResult.getString("me").getValue() : null)
                                .hosts(listToSet(helloResult.getArray("hosts", new BsonArray())))
                                .passives(listToSet(helloResult.getArray("passives", new BsonArray())))
                                .arbiters(listToSet(helloResult.getArray("arbiters", new BsonArray())))
                                .primary(getString(helloResult, "primary"))
                                .maxDocumentSize(getMaxBsonObjectSize(helloResult))
                                .tagSet(getTagSetFromDocument(helloResult.getDocument("tags", new BsonDocument())))
                                .setName(getString(helloResult, "setName"))
                                .minWireVersion(getMinWireVersion(helloResult))
                                .maxWireVersion(getMaxWireVersion(helloResult))
                                .electionId(getElectionId(helloResult))
                                .setVersion(getSetVersion(helloResult))
                                .topologyVersion(getTopologyVersion(helloResult))
                                .lastWriteDate(getLastWriteDate(helloResult))
                                .roundTripTime(roundTripTime, NANOSECONDS)
                                .logicalSessionTimeoutMinutes(getLogicalSessionTimeoutMinutes(helloResult))
                                .helloOk(helloResult.getBoolean("helloOk", BsonBoolean.FALSE).getValue())
                                .ok(CommandHelper.isCommandOk(helloResult)).build();
    }

    private static int getMinWireVersion(final BsonDocument helloResult) {
        return helloResult.getInt32("minWireVersion", new BsonInt32(getDefaultMinWireVersion())).getValue();
    }

    private static int getMaxWireVersion(final BsonDocument helloResult) {
        return helloResult.getInt32("maxWireVersion", new BsonInt32(getDefaultMaxWireVersion())).getValue();
    }

    private static Date getLastWriteDate(final BsonDocument helloResult) {
        if (!helloResult.containsKey("lastWrite")) {
            return null;
        }
        return new Date(helloResult.getDocument("lastWrite").getDateTime("lastWriteDate").getValue());
    }

    private static ObjectId getElectionId(final BsonDocument helloResult) {
        return helloResult.containsKey("electionId") ? helloResult.getObjectId("electionId").getValue() : null;
    }

    private static Integer getSetVersion(final BsonDocument helloResult) {
        return helloResult.containsKey("setVersion") ? helloResult.getNumber("setVersion").intValue() : null;
    }

    private static TopologyVersion getTopologyVersion(final BsonDocument helloResult) {
        return helloResult.containsKey("topologyVersion") && helloResult.get("topologyVersion").isDocument()
                ? new TopologyVersion(helloResult.getDocument("topologyVersion")) : null;
    }

    private static ObjectId getServiceId(final BsonDocument helloResult) {
        return helloResult.containsKey("serviceId") && helloResult.get("serviceId").isObjectId()
                ? helloResult.getObjectId("serviceId").getValue() : null;
    }

    private static int getMaxMessageSizeBytes(final BsonDocument helloResult) {
        return helloResult.getInt32("maxMessageSizeBytes", new BsonInt32(getDefaultMaxMessageSize())).getValue();
    }

    private static int getMaxBsonObjectSize(final BsonDocument helloResult) {
        return helloResult.getInt32("maxBsonObjectSize", new BsonInt32(getDefaultMaxDocumentSize())).getValue();
    }

    private static int getMaxWriteBatchSize(final BsonDocument helloResult) {
        return helloResult.getInt32("maxWriteBatchSize", new BsonInt32(getDefaultMaxWriteBatchSize())).getValue();
    }

    private static Integer getLogicalSessionTimeoutMinutes(final BsonDocument helloResult) {
        return helloResult.isNumber("logicalSessionTimeoutMinutes")
                       ? helloResult.getNumber("logicalSessionTimeoutMinutes").intValue() : null;
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

    private static ServerType getServerType(final BsonDocument helloResult) {

        if (!CommandHelper.isCommandOk(helloResult)) {
            return UNKNOWN;
        }

        if (isReplicaSetMember(helloResult)) {

            if (helloResult.getBoolean("hidden", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_OTHER;
            }

            if (helloResult.getBoolean("isWritablePrimary", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_PRIMARY;
            }

            if (helloResult.getBoolean(LEGACY_HELLO_LOWER, BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_PRIMARY;
            }

            if (helloResult.getBoolean("secondary", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_SECONDARY;
            }

            if (helloResult.getBoolean("arbiterOnly", BsonBoolean.FALSE).getValue()) {
                return REPLICA_SET_ARBITER;
            }

            if (helloResult.containsKey("setName") && helloResult.containsKey("hosts")) {
                return ServerType.REPLICA_SET_OTHER;
            }

            return ServerType.REPLICA_SET_GHOST;
        }

        if (helloResult.containsKey("msg") && helloResult.get("msg").equals(new BsonString("isdbgrid"))) {
            return SHARD_ROUTER;
        }

        return STANDALONE;
    }

    private static boolean isReplicaSetMember(final BsonDocument helloResult) {
        return helloResult.containsKey("setName") || helloResult.getBoolean("isreplicaset", BsonBoolean.FALSE).getValue();
    }

    private static TagSet getTagSetFromDocument(final BsonDocument tagsDocuments) {
        List<Tag> tagList = new ArrayList<Tag>();
        for (final Map.Entry<String, BsonValue> curEntry : tagsDocuments.entrySet()) {
            tagList.add(new Tag(curEntry.getKey(), curEntry.getValue().asString().getValue()));
        }
        return new TagSet(tagList);
    }

    private static List<String> getCompressors(final BsonDocument helloResult) {
        List<String> compressorList = new ArrayList<String>();
        for (BsonValue compressor : helloResult.getArray("compression", new BsonArray())) {
            compressorList.add(compressor.asString().getValue());
        }
        return compressorList;
    }

    private DescriptionHelper() {
    }
}

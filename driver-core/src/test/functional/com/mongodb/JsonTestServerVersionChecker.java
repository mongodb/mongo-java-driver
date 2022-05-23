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
package com.mongodb;

import com.mongodb.connection.ServerVersion;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.List;

import static com.mongodb.ClusterFixture.getServerVersion;
import static com.mongodb.ClusterFixture.getVersionList;
import static com.mongodb.ClusterFixture.isAuthenticated;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isLoadBalanced;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class JsonTestServerVersionChecker {
    private static final List<String> TOPOLOGY_TYPES = asList("sharded", "sharded-replicaset", "replicaset", "single", "load-balanced");

    public static boolean skipTest(final BsonDocument testDocument, final BsonDocument testDefinition) {
        return skipTest(testDocument, testDefinition, getServerVersion());
    }

    public static boolean skipTest(final BsonDocument testDocument, final BsonDocument testDefinition, final ServerVersion serverVersion) {
        return !(canRunTest(testDocument, serverVersion) && canRunTest(testDefinition, serverVersion));
    }

    private static boolean canRunTest(final BsonDocument document, final ServerVersion serverVersion) {
        if ((!serverlessMatches(document.getString("serverless", new BsonString("allow")).getValue()))) {
            return false;
        }

        if (document.containsKey("minServerVersion")
                && serverVersion.compareTo(getMinServerVersionForField("minServerVersion", document)) < 0) {
            return false;
        }
        if (document.containsKey("maxServerVersion")
                && serverVersion.compareTo(getMaxServerVersionForField("maxServerVersion", document)) > 0) {
            return false;
        }
        if (document.containsKey("topology") && !topologyMatches(document.getArray("topology"))) {
            return false;
        }
        if (document.containsKey("topologies") && !topologyMatches(document.getArray("topologies"))) {
            return false;
        }
        if (document.containsKey("authEnabled") && (isAuthenticated() != document.getBoolean("authEnabled").getValue())) {
            return false;
        }

        if (document.containsKey("runOn")) {
            return canRunTest(document.getArray("runOn"), serverVersion);
        }

        // Ignore certain matching types
        if (document.containsKey("ignore_if_server_version_less_than")
                && serverVersion.compareTo(getMinServerVersionForField("ignore_if_server_version_less_than", document)) < 0) {
            return false;
        }
        if (document.containsKey("ignore_if_server_version_greater_than")
                && serverVersion.compareTo(getMaxServerVersionForField("ignore_if_server_version_greater_than", document)) > 0) {
            return false;
        }
        if (document.containsKey("ignore_if_topology_type") && topologyMatches(document.getArray("ignore_if_topology_type"))) {
            return false;
        }

        return true;
    }

    private static boolean canRunTest(final BsonArray runOn, final ServerVersion serverVersion) {
        return runOn.stream().anyMatch(v -> canRunTest(v.asDocument(), serverVersion));
    }

    public static boolean topologyMatches(final BsonArray topologyTypes) {
        for (BsonValue type : topologyTypes) {
            String typeString = type.asString().getValue();
            if ((typeString.equals("sharded") || typeString.equals("sharded-replicaset")) && isSharded()) {
                return true;
            } else if (typeString.equals("replicaset") && isDiscoverableReplicaSet()) {
                return true;
            } else if (typeString.equals("single") && isStandalone()) {
                return true;
            } else if (typeString.equals("load-balanced") && isLoadBalanced()) {
                return true;
            } else if (!TOPOLOGY_TYPES.contains(typeString)) {
                throw new IllegalArgumentException(format("Unexpected topology type: '%s'", typeString));
            }
        }
        return false;
    }

    public static boolean serverlessMatches(final String serverlessRequirement) {
        switch (serverlessRequirement) {
            case "require":
                return ClusterFixture.isServerlessTest();
            case "forbid":
                return !ClusterFixture.isServerlessTest();
            case "allow":
                return true;
            default:
                throw new UnsupportedOperationException("Unsupported serverless requirement value: " + serverlessRequirement);
        }
    }

    public static ServerVersion getMinServerVersionForField(final String fieldName, final BsonDocument document) {
        return getMinServerVersion(document.getString(fieldName).getValue());
    }

    public static ServerVersion getMinServerVersion(final String serverVersion) {
        return new ServerVersion(getVersionList(serverVersion));
    }

    public static ServerVersion getMaxServerVersionForField(final String fieldName, final BsonDocument document) {
        return getMaxServerVersionForField(document.getString(fieldName).getValue());
    }

    public static ServerVersion getMaxServerVersionForField(final String serverVersion) {
        List<Integer> versionList = getVersionList(serverVersion);
        if (versionList.size() > 2 && versionList.get(2).equals(0)) {
            versionList = asList(versionList.get(0), versionList.get(1), Integer.MAX_VALUE);
        }
        return new ServerVersion(versionList);
    }


    private JsonTestServerVersionChecker() {
    }
}

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
import org.bson.BsonValue;

import java.util.List;

import static com.mongodb.ClusterFixture.getServerVersion;
import static com.mongodb.ClusterFixture.getVersionList;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class JsonTestServerVersionChecker {
    private static final List<String> TOPOLOGY_TYPES = asList("sharded", "replicaset", "single");

    public static boolean skipTest(final BsonDocument testDocument, final BsonDocument testDefinition) {
        return skipTest(testDocument, testDefinition, getServerVersion());
    }

    public static boolean skipTest(final BsonDocument testDocument, final BsonDocument testDefinition, final ServerVersion serverVersion) {
        return !(canRunTest(testDocument, serverVersion) && canRunTest(testDefinition, serverVersion));
    }

    private static boolean canRunTest(final BsonDocument document, final ServerVersion serverVersion) {
        if (document.containsKey("minServerVersion")
                && serverVersion.compareTo(getMinServerVersionForField("minServerVersion", document)) < 0) {
            return false;
        }
        if (document.containsKey("maxServerVersion")
                && serverVersion.compareTo(getMaxServerVersionForField("maxServerVersion", document)) > 0) {
            return false;
        }
        if (document.containsKey("topology")) {
            BsonArray topologyTypes = document.getArray("topology");
            return topologyMatches(topologyTypes);
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
        if (document.containsKey("ignore_if_topology_type")) {
            return !topologyMatches(document.getArray("ignore_if_topology_type"));
        }

        return true;
    }

    private static boolean canRunTest(final BsonArray runOn, final ServerVersion serverVersion) {
        boolean topologyFound = false;
        for (BsonValue info : runOn) {
            topologyFound = canRunTest(info.asDocument(), serverVersion);
            if (topologyFound) {
                break;
            }
        }
        return topologyFound;
    }

    private static boolean topologyMatches(final BsonArray topologyTypes) {
        for (BsonValue type : topologyTypes) {
            String typeString = type.asString().getValue();
            if (typeString.equals("sharded") && isSharded()) {
                return true;
            } else if (typeString.equals("replicaset") && isDiscoverableReplicaSet()) {
                return true;
            } else if (typeString.equals("single") && isStandalone()) {
                return true;
            } else if (!TOPOLOGY_TYPES.contains(typeString)) {
                throw new IllegalArgumentException(format("Unexpected topology type: '%s'", typeString));
            }
        }
        return false;
    }

    private static ServerVersion getMinServerVersionForField(final String fieldName, final BsonDocument document) {
        return new ServerVersion(getVersionList(document.getString(fieldName).getValue()));
    }

    private static ServerVersion getMaxServerVersionForField(final String fieldName, final BsonDocument document) {
        List<Integer> versionList = getVersionList(document.getString(fieldName).getValue());
        if (versionList.size() > 2 && versionList.get(2).equals(0)) {
            versionList = asList(versionList.get(0), versionList.get(1), Integer.MAX_VALUE);
        }
        return new ServerVersion(versionList);
    }


    private JsonTestServerVersionChecker() {
    }
}

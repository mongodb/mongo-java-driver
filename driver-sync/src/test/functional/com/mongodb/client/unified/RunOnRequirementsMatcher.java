/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.MongoClientSettings;
import com.mongodb.connection.ServerVersion;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;
import java.util.Objects;

import static com.mongodb.ClusterFixture.getServerParameters;
import static com.mongodb.JsonTestServerVersionChecker.getMaxServerVersionForField;
import static com.mongodb.JsonTestServerVersionChecker.getMinServerVersion;
import static com.mongodb.JsonTestServerVersionChecker.serverlessMatches;
import static com.mongodb.JsonTestServerVersionChecker.topologyMatches;

final class RunOnRequirementsMatcher {
    public static boolean runOnRequirementsMet(final BsonArray runOnRequirements, final MongoClientSettings clientSettings,
                                               final ServerVersion serverVersion) {
        for (BsonValue cur : runOnRequirements) {
            boolean requirementMet = true;
            BsonDocument requirement = cur.asDocument();

            requirementLoop:
            for (Map.Entry<String, BsonValue> curRequirement : requirement.entrySet()) {
                switch (curRequirement.getKey()) {
                    case "minServerVersion":
                        if (serverVersion.compareTo(getMinServerVersion(curRequirement.getValue().asString().getValue())) < 0) {
                            requirementMet = false;
                            break requirementLoop;
                        }
                        break;
                    case "maxServerVersion":
                        if (serverVersion.compareTo(getMaxServerVersionForField(curRequirement.getValue().asString().getValue())) > 0) {
                            requirementMet = false;
                            break requirementLoop;
                        }
                        break;
                    case "topologies":
                        BsonArray topologyTypes = curRequirement.getValue().asArray();
                        if (!topologyMatches(topologyTypes)) {
                            requirementMet = false;
                            break requirementLoop;
                        }
                        break;
                    case "serverless":
                        if (!serverlessMatches(curRequirement.getValue().asString().getValue())) {
                             requirementMet = false;
                             break requirementLoop;
                        }
                        break;
                    case "auth":
                        if (curRequirement.getValue().asBoolean().getValue() == (clientSettings.getCredential() == null)) {
                            requirementMet = false;
                            break requirementLoop;
                        }
                        break;
                    case "serverParameters":
                        BsonDocument serverParameters = getServerParameters();
                        for (Map.Entry<String, BsonValue> curParameter: curRequirement.getValue().asDocument().entrySet()) {
                            if (!Objects.equals(serverParameters.get(curParameter.getKey()), curParameter.getValue())) {
                                requirementMet = false;
                                break requirementLoop;
                            }
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported runOnRequirement: " + curRequirement.getKey());
                }
            }

            if (requirementMet) {
                return true;
            }
        }
        return false;
    }

    private RunOnRequirementsMatcher() {
    }
}

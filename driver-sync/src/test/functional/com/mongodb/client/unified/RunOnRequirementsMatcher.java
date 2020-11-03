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

import com.mongodb.connection.ServerVersion;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import static com.mongodb.JsonTestServerVersionChecker.getMaxServerVersionForField;
import static com.mongodb.JsonTestServerVersionChecker.getMinServerVersionForField;
import static com.mongodb.JsonTestServerVersionChecker.topologyMatches;

final class RunOnRequirementsMatcher {
    public static boolean runOnRequirementsMet(final BsonArray runOnRequirements, final ServerVersion serverVersion) {
        for (BsonValue cur : runOnRequirements) {
            boolean requirementMet = true;
            BsonDocument requirement = cur.asDocument();
            if (requirement.containsKey("minServerVersion")
                    && serverVersion.compareTo(getMinServerVersionForField("minServerVersion", requirement)) < 0) {
                requirementMet = false;
            }
            if (requirement.containsKey("maxServerVersion")
                    && serverVersion.compareTo(getMaxServerVersionForField("maxServerVersion", requirement)) > 0) {
                requirementMet = false;
            }

            if (requirement.containsKey("topologies")) {
                BsonArray topologyTypes = requirement.getArray("topologies");
                if (!topologyMatches(topologyTypes)) {
                    requirementMet = false;
                }
            }
            if (requirementMet) {
                return true;
            }
        }
        return false;
    }

    private RunOnRequirementsMatcher(){
    }
}

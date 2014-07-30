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

package com.mongodb.operation;

import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import org.mongodb.Document;

import java.util.HashSet;
import java.util.Set;

import static com.mongodb.connection.ClusterConnectionMode.SINGLE;

/**
 * Help for managing read preferences for commands.
 */
final class CommandReadPreferenceHelper {
    private static final Set<String> OBEDIENT_COMMANDS = new HashSet<String>();

    static {
        OBEDIENT_COMMANDS.add("aggregate");
        OBEDIENT_COMMANDS.add("collstats");
        OBEDIENT_COMMANDS.add("dbstats");
        OBEDIENT_COMMANDS.add("count");
        OBEDIENT_COMMANDS.add("group");
        OBEDIENT_COMMANDS.add("distinct");
        OBEDIENT_COMMANDS.add("geonear");
        OBEDIENT_COMMANDS.add("geosearch");
        OBEDIENT_COMMANDS.add("geowalk");
        OBEDIENT_COMMANDS.add("text");
    }

    /**
     * Returns true if the command is a query in disguise.
     *
     * @param command the Document containing the details of the command
     * @return true if the command is a query, false otherwise.
     */
    public static boolean isQuery(final Document command) {
        return !isPrimaryRequired(command);
    }

    /**
     * Returns the recommended read preference for the given command when run against a cluster with the given description.
     *
     * @param commandDocument    the Document describing the command to run
     * @param readPreference     the ReadPreference requested for the command
     * @param clusterDescription the cluster description
     * @return the recommended read preference for the given command when run against a cluster with the given description
     */
    public static ReadPreference getCommandReadPreference(final Document commandDocument, final ReadPreference readPreference,
                                                          final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == SINGLE || clusterDescription.getType() != ClusterType.REPLICA_SET) {
            return readPreference;
        }

        if (isPrimaryRequired(commandDocument)) {
            return ReadPreference.primary();
        } else {
            return readPreference;
        }
    }

    private static boolean isPrimaryRequired(final Document commandDocument) {
        String commandName = commandDocument.keySet().iterator().next().toLowerCase();

        boolean primaryRequired;

        // explicitly check for inline mapreduce commands
        if (commandName.equals("mapreduce")) {
            primaryRequired = !(commandDocument.get("out") instanceof Document)
                              || ((Document) commandDocument.get("out")).get("inline") == null;
        } else {
            primaryRequired = !OBEDIENT_COMMANDS.contains(commandName);
        }
        return primaryRequired;
    }

    private CommandReadPreferenceHelper() {
    }
}

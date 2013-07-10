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

package org.mongodb.operation;

import org.mongodb.Document;
import org.mongodb.ReadPreference;
import org.mongodb.command.Command;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ClusterType;

import java.util.HashSet;
import java.util.Set;

import static org.mongodb.connection.ClusterConnectionMode.Direct;

/**
 * Help for managing read preferences for commands.
 */
// TODO: Move me
public final class CommandReadPreferenceHelper {
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
     * @param command the command
     * @return true if the command is a query, false otherwise.
     */
    public static boolean isQuery(final Command command) {
       return !isPrimaryRequired(command);
    }

    /**
     * Returns the recommended read preference for the given command when run against a cluster with the given description.
     *
     * @param command the command
     * @param clusterDescription the cluster description
     * @return the recommended read preference for the given command when run against a cluster with the given description
     */
    public static ReadPreference getCommandReadPreference(final Command command, final ClusterDescription clusterDescription) {
        if (clusterDescription.getMode() == Direct || clusterDescription.getType() != ClusterType.ReplicaSet) {
            return command.getReadPreference();
        }

        boolean primaryRequired = isPrimaryRequired(command);

        if (primaryRequired) {
            return ReadPreference.primary();
        }
        else {
            return command.getReadPreference();
        }
    }

    private static boolean isPrimaryRequired(final Command command) {
        Document commandDocument = command.toDocument();
        String commandName = commandDocument.keySet().iterator().next().toLowerCase();

        boolean primaryRequired;

        // explicitly check for inline mapreduce commands
        if (commandName.equals("mapreduce")) {
            primaryRequired = !(commandDocument.get("out") instanceof Document)
                    || ((Document) commandDocument.get("out")).get("inline") == null;
        }
        else {
            primaryRequired = !OBEDIENT_COMMANDS.contains(commandName);
        }
        return primaryRequired;
    }

    private CommandReadPreferenceHelper() {
    }
}

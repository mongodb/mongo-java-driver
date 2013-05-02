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

package com.mongodb;

/**
 * Container for the result of aggregation operation.
 */
public class AggregationOutput {
    protected final CommandResult commandResult;
    protected final DBObject command;

    /**
     * Create new container. This class should be hidden, so don't use it in your code.
     *
     * @param command       command, used to perform the operation
     * @param commandResult result of the operation
     */
    public AggregationOutput(final DBObject command, final CommandResult commandResult) {
        if (!commandResult.containsField("result") && !(command.get("result") instanceof Iterable)) {
            throw new IllegalArgumentException("Result undefined");
        }
        this.commandResult = commandResult;
        this.command = command;
    }

    /**
     * Returns the results of the aggregation.
     *
     * @return iterable collection of {@link DBObject}
     */
    @SuppressWarnings("unchecked")
    public Iterable<DBObject> results() {
        return (Iterable<DBObject>) commandResult.get("result");
    }

    /**
     * Returns the command result of the aggregation.
     *
     * @return aggregation command result
     */
    public CommandResult getCommandResult() {
        return commandResult;
    }

    /**
     * Returns the original aggregation command.
     *
     * @return a command document
     */
    public DBObject getCommand() {
        return command;
    }

    /**
     * Returns the address of the server used to execute the aggregation.
     *
     * @return address of the server
     */
    public ServerAddress getServerUsed() {
        return commandResult.getServerUsed();
    }

    @Override
    public String toString() {
        return commandResult.toString();
    }
}

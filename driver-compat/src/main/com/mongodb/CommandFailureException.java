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

package com.mongodb;

import org.mongodb.MongoCommandFailureException;

public class CommandFailureException extends MongoException {
    private static final long serialVersionUID = -1180715413196161037L;
    private final CommandResult commandResult;

    /**
     * Construct a new instance with the CommandResult from a failed command
     *
     * @param commandResult the result of running the command
     */
    public CommandFailureException(final CommandResult commandResult, final String message) {
        super(ServerError.getCode(commandResult), message);
        this.commandResult = commandResult;
    }

    /**
     * Construct a new instance with the CommandResult from a failed command
     *
     * @param commandResult the result of running the command
     */
    public CommandFailureException(final CommandResult commandResult) {
        super(ServerError.getCode(commandResult), commandResult.toString());
        this.commandResult = commandResult;
    }

    CommandFailureException(final MongoCommandFailureException e) {
        this(new CommandResult(DBObjects.toDBObject(e.getResponse()), new ServerAddress(e.getServerAddress())),
             e.getMessage());
    }

    /**
     * Gets the getlasterror command result document.
     *
     * @return the command result
     */
    public CommandResult getCommandResult() {
        return commandResult;
    }
}

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

package org.mongodb.command;

import org.mongodb.CommandResult;
import org.mongodb.operation.MongoServerException;

import static java.lang.String.format;

/**
 * Exception thrown when a command fails.
 */
public class MongoCommandFailureException extends MongoServerException {
    private static final long serialVersionUID = -50109343643507362L;

    private final CommandResult commandResult;

    public MongoCommandFailureException(final CommandResult commandResult) {
        super(format("Command failed with error %s: '%s' on server %s", commandResult.getErrorCode(),
                     commandResult.getErrorMessage(), commandResult.getAddress()), commandResult.getAddress());
        this.commandResult = commandResult;
    }

    public CommandResult getCommandResult() {
        return commandResult;
    }

    @Override
    public int getErrorCode() {
        return commandResult.getErrorCode();
    }

    @Override
    public String getErrorMessage() {
        return commandResult.getErrorMessage();
    }
}

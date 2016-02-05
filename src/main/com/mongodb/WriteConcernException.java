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

/**
 * An exception representing an error reported due to a write failure.
 */
public class WriteConcernException extends MongoException {

    private static final long serialVersionUID = 841056799207039974L;

    private final CommandResult commandResult;

    /**
     * Construct a new instance with the CommandResult from getlasterror command
     *
     * @param commandResult the command result
     * @deprecated for internal use only, this constructor will be removed in the next major release
     */
    @Deprecated
    public WriteConcernException(final CommandResult commandResult) {
        this(commandResult.getCode(), commandResult);
    }

    WriteConcernException(final int code, final CommandResult commandResult) {
        super(code, commandResult.toString());
        this.commandResult = commandResult;
    }

    /**
     * Gets the address of the server that the write executed on.
     *
     * @return the address of the server that the write executed on
     * @since 2.13
     */
    public ServerAddress getServerAddress() {
        return commandResult.getServerUsed();
    }

    /**
     * Gets the error message associated with the write failure.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        if (commandResult.containsField("err")) {
            return commandResult.getString("err");
        } else if (commandResult.containsField("errmsg")) {
            return commandResult.getString("errmsg");
        } else {
            return null;
        }
    }

    /**
     * Gets the getlasterror command result document.
     *
     * @return the command result
     * @deprecated Use either {@link #getErrorMessage()} or {@link #getCode()} or {@link #getServerAddress()}
     */
    @Deprecated
    public CommandResult getCommandResult() {
        return commandResult;
    }
}

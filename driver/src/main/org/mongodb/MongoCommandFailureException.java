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

package org.mongodb;

import org.bson.BsonDocument;
import org.mongodb.connection.ServerAddress;

import static java.lang.String.format;

/**
 * Exception thrown when a command fails.
 */
public class MongoCommandFailureException extends MongoServerException {
    private static final long serialVersionUID = -50109343643507362L;

    private final BsonDocument response;
    private final int errorCode;
    private final String errorMessage;

    public MongoCommandFailureException(final BsonDocument response, final int errorCode, final String errorMessage,
                                        final ServerAddress serverAddress) {
        super(format("Command failed with error %s: '%s' on server %s", errorCode, errorMessage, serverAddress), serverAddress);
        this.response = response;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public <T> MongoCommandFailureException(final CommandResult<T> commandResult) {
        this(commandResult.getRawResponse(), commandResult.getErrorCode(), commandResult.getErrorMessage(), commandResult.getAddress());
    }

    public BsonDocument getResponse() {
        return response;
    }

    @Override
    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }
}

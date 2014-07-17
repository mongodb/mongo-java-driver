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

import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;
import org.mongodb.CommandResult;

import java.io.StringWriter;

import static java.lang.String.format;

/**
 * An exception indicating that a command sent to a MongoDB server returned a failure.
 */
public class CommandFailureException extends MongoServerException {
    private static final long serialVersionUID = -1180715413196161037L;
    private final CommandResult commandResult;

    /**
     * Construct a new instance with the CommandResult from a failed command
     *
     * @param commandResult the result of running the command
     */
    public CommandFailureException(final CommandResult commandResult) {
        super(format("Command failed with error %s: '%s' on server %s. The full response is %s", commandResult.getErrorCode(),
                     commandResult.getErrorMessage(), commandResult.getAddress(),
                     getResponseAsJson(commandResult.getResponse())),
              commandResult.getAddress());
        this.commandResult = commandResult;
    }

    @Override
    public int getErrorCode() {
        return commandResult.getErrorCode();
    }

    @Override
    public String getErrorMessage() {
        return commandResult.getErrorMessage();
    }

    /**
     * Gets the command result.
     *
     * @return the command result
     * @since 3.0
     */
    public CommandResult getResult() {
        return commandResult;
    }

    private static String getResponseAsJson(final BsonDocument commandResponse) {
        StringWriter writer = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(writer);
        new BsonDocumentCodec().encode(jsonWriter, commandResponse, EncoderContext.builder().build());
        return writer.toString();
    }
}

/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A simple wrapper to hold the result of a command.  All the fields from the response document have been added to this result.
 *
 * @mongodb.driver.manual reference/command/ Database Commands
 */
public class CommandResult extends BasicDBObject {
    private static final long serialVersionUID = 5907909423864204060L;

    /**
     * The response document.
     */
    private final BsonDocument response;
    /**
     * The server address.
     */
    private final ServerAddress address;

    CommandResult(final BsonDocument response, final Decoder<DBObject> decoder) {
        this(response, decoder, null);
    }

    CommandResult(final BsonDocument response, final Decoder<DBObject> decoder, @Nullable final ServerAddress address) {
        this.address = address;
        this.response = notNull("response", response);
        putAll(decoder.decode(new BsonDocumentReader(response), DecoderContext.builder().build()));
    }

    /**
     * Gets the "ok" field, which is whether this command executed correctly or not.
     *
     * @return true if the command executed without error.
     */
    public boolean ok() {
        Object okValue = get("ok");
        if (okValue instanceof Boolean) {
            return (Boolean) okValue;
        } else if (okValue instanceof Number) {
            return ((Number) okValue).intValue() == 1;
        } else {
            return false;
        }
    }

    /**
     * Gets the error message associated with a failed command.
     *
     * @return The error message or null
     */
    @Nullable
    public String getErrorMessage() {
        Object foo = get("errmsg");
        if (foo == null) {
            return null;
        }
        return foo.toString();
    }

    /**
     * Utility method to create an exception from a failed command.
     *
     * @return The mongo exception, or null if the command was successful.
     */
    @Nullable
    public MongoException getException() {
        if (!ok()) {
            return createException();
        }

        return null;
    }

    /**
     * Throws a {@code CommandFailureException} if the command failed. Otherwise, returns normally.
     *
     * @throws MongoException with the exception from the failed command
     * @see #ok()
     */
    public void throwOnError() {
        if (!ok()) {
            throw createException();
        }
    }

    private MongoException createException() {
        return new MongoCommandException(response, address);
    }
}

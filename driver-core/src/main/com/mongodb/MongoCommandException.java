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

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;

import java.io.StringWriter;

import static java.lang.String.format;

/**
 * An exception indicating that a command sent to a MongoDB server returned a failure.
 *
 * @since 2.13
 */
public class MongoCommandException extends MongoServerException {
    private static final long serialVersionUID = 8160676451944215078L;

    private final BsonDocument response;

    /**
     * Construct a new instance with the CommandResult from a failed command
     *
     * @param response the command response
     * @param address the address of the server that generated the response
     */
    public MongoCommandException(final BsonDocument response, final ServerAddress address) {
        super(extractErrorCode(response),
              format("Command failed with error %s: '%s' on server %s. The full response is %s", extractErrorCodeAndName(response),
                     extractErrorMessage(response), address, getResponseAsJson(response)), address);
        this.response = response;
    }

    /**
     * Gets the error code associated with the command failure.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return getCode();
    }

    /**
     * Gets the name associated with the error code.
     *
     * @return the error code name, which may be the empty string
     * @since 3.8
     * @mongodb.server.release 3.4
     */
    public String getErrorCodeName() {
        return extractErrorCodeName(response);
    }

    /**
     * Gets the error message associated with the command failure.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        return extractErrorMessage(response);
    }

    /**
     * For internal use only.
     *
     * @return the full response to the command failure.
     */
    public BsonDocument getResponse() {
        return response;
    }

    private static String getResponseAsJson(final BsonDocument commandResponse) {
        StringWriter writer = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(writer);
        new BsonDocumentCodec().encode(jsonWriter, commandResponse, EncoderContext.builder().build());
        return writer.toString();
    }

    private static String extractErrorCodeAndName(final BsonDocument response) {
        int errorCode = extractErrorCode(response);
        String errorCodeName = extractErrorCodeName(response);
        if (errorCodeName.isEmpty()) {
            return Integer.toString(errorCode);
        } else {
            return String.format("%d (%s)", errorCode, errorCodeName);
        }
    }

    private static int extractErrorCode(final BsonDocument response) {
        return response.getNumber("code", new BsonInt32(-1)).intValue();
    }

    private static String extractErrorCodeName(final BsonDocument response) {
        return response.getString("codeName", new BsonString("")).getValue();
    }

    private static String extractErrorMessage(final BsonDocument response) {
        String errorMessage = response.getString("errmsg", new BsonString("")).getValue();
        // Satisfy nullability checker
        if (errorMessage == null) {
            throw new MongoInternalException("This value should not be null");
        }
        return errorMessage;
    }
}

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
import org.bson.BsonInt32;
import org.bson.BsonValue;

import static java.lang.String.format;

/**
 * Exception indicating a failure to successfully complete a write operation according to the WriteConcern used for the operation.
 *
 * @since 3.0
 */
public class MongoWriteException extends MongoServerException {
    private static final long serialVersionUID = -1139302724723542251L;

    private final BsonDocument response;

    /**
     * Construct a new instance.
     *
     * @param response the response to the write operation
     * @param address the address of the server that executed the operation.
     */
    public MongoWriteException(final BsonDocument response, final ServerAddress address) {
        super(format("Write failed with error code %d and error message '%s'", extractErrorCode(response), extractErrorMessage(response)),
              address);
        this.response = response;
    }

    @Override
    public int getErrorCode() {
        return extractErrorCode(response);
    }

    @Override
    public String getErrorMessage() {
        return extractErrorMessage(response);
    }

    /**
     * Gets the response to the write operation.
     *
     * @return the response to the write operation
     */
    public BsonDocument getResponse() {
        return response;
    }

    /**
     * For internal use only: extract the error code from the response to a getlasterror command.
     * @param response the response
     * @return the code, or -1 if there is none
     */
    public static int extractErrorCode(final BsonDocument response) {
        // mongos may set an err field containing duplicate key error information
        if (response.containsKey("err")) {
            String errorMessage = extractErrorMessage(response);
            if (errorMessage.contains("E11000 duplicate key error")) {
                return 11000;
            }
        }

        // mongos may return a list of documents representing getlasterror responses from each shard.  Return the one with a matching
        // "err" field, so that it can be used to get the error code
        if (!response.containsKey("code") && response.containsKey("errObjects")) {
            for (BsonValue curErrorDocument : response.getArray("errObjects")) {
                if (extractErrorMessage(response).equals(extractErrorMessage(curErrorDocument.asDocument()))) {
                    return curErrorDocument.asDocument().getNumber("code").intValue();
                }
            }
        }
        return response.getNumber("code", new BsonInt32(-1)).intValue();
    }

    /**
     * For internal use only: extract the error message from the response to a getlasterror command.
     *
     * @param response the response
     * @return the error message
     */
    public static String extractErrorMessage(final BsonDocument response) {
        if (response.isString("err")) {
            return response.getString("err").getValue();
        } else {
            return null;
        }
    }
}

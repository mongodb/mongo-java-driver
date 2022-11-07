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
import org.bson.BsonInt32;
import org.bson.BsonString;

/**
 * An exception indicating that a query operation failed on the server.
 *
 * @since 3.0
 * @serial exclude
 */
public class MongoQueryException extends MongoCommandException {
    private static final long serialVersionUID = -5113350133297015801L;

    /**
     * Construct an instance.
     *
     * @param response the server response document
     * @param serverAddress the server address
     * @since 4.8
     */
    public MongoQueryException(final BsonDocument response, final ServerAddress serverAddress) {
        super(response, serverAddress);
    }

    /**
     * Construct an instance.
     *
     * @param address the server address
     * @param errorCode the error code
     * @param errorMessage the error message
     * @deprecated Prefer {@link #MongoQueryException(BsonDocument, ServerAddress)}
     */
    @Deprecated
    public MongoQueryException(final ServerAddress address, final int errorCode, final String errorMessage) {
        this(manufactureResponse(errorCode, null, errorMessage), address);
    }

    /**
     * Construct an instance.
     *
     * @param address the server address
     * @param errorCode the error code
     * @param errorCodeName the error code name
     * @param errorMessage the error message
     * @since 4.6
     * @deprecated Prefer {@link #MongoQueryException(BsonDocument, ServerAddress)}
     */
    @Deprecated
    public MongoQueryException(final ServerAddress address, final int errorCode, @Nullable final String errorCodeName,
            final String errorMessage) {
        this(manufactureResponse(errorCode, errorCodeName, errorMessage), address);
    }

    /**
     * Construct an instance from a command exception.
     *
     * @param commandException the command exception
     * @since 3.7
     * @deprecated Prefer {@link #MongoQueryException(BsonDocument, ServerAddress)}
     */
    @Deprecated
    public MongoQueryException(final MongoCommandException commandException) {
        this(commandException.getResponse(), commandException.getServerAddress());
    }

    private static BsonDocument manufactureResponse(final int errorCode, @Nullable final String errorCodeName, final String errorMessage) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(1))
                .append("code", new BsonInt32(errorCode))
                .append("errmsg", new BsonString(errorMessage));
        if (errorCodeName != null) {
            response.append("codeName", new BsonString(errorCodeName));
        }
        return response;
    }
}

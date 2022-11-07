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

/**
 * Subclass of {@link MongoException} representing a cursor-not-found exception.
 *
 * @since 2.12
 * @serial exclude
 */
public class MongoCursorNotFoundException extends MongoQueryException {

    private static final long serialVersionUID = -4415279469780082174L;

    private final long cursorId;

    /**
     * Construct an instance.
     *
     * @param cursorId      cursor identifier
     * @param response      the server response document
     * @param serverAddress the server address
     * @since 4.8
     */
    public MongoCursorNotFoundException(final long cursorId, final BsonDocument response, final ServerAddress serverAddress) {
        super(response, serverAddress);
        this.cursorId = cursorId;
    }

    /**
     * Construct a new instance.
     *
     * @param cursorId      cursor identifier
     * @param serverAddress server address
     * @deprecated Prefer {@link #MongoCursorNotFoundException(long, BsonDocument, ServerAddress)}
     */
    @Deprecated
    public MongoCursorNotFoundException(final long cursorId, final ServerAddress serverAddress) {
        super(serverAddress, -5, "Cursor " + cursorId + " not found on server " + serverAddress);
        this.cursorId = cursorId;
    }

    /**
     * Get the cursor id that wasn't found.
     *
     * @return the ID of the cursor
     */
    public long getCursorId() {
        return cursorId;
    }
}

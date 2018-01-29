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

/**
 * Subclass of {@link MongoException} representing a cursor-not-found exception.
 *
 * @since 2.12
 */
public class MongoCursorNotFoundException extends MongoQueryException {

    private static final long serialVersionUID = -4415279469780082174L;

    private final long cursorId;
    private final ServerAddress serverAddress;

    /**
     * Construct a new instance.
     *
     * @param cursorId      cursor identifier
     * @param serverAddress server address
     */
    public MongoCursorNotFoundException(final long cursorId, final ServerAddress serverAddress) {
        super(serverAddress, -5, "Cursor " + cursorId + " not found on server " + serverAddress);
        this.cursorId = cursorId;
        this.serverAddress = serverAddress;
    }

    /**
     * Get the cursor id that wasn't found.
     *
     * @return the ID of the cursor
     */
    public long getCursorId() {
        return cursorId;
    }

    /**
     * The server address where the cursor is.
     *
     * @return the ServerAddress representing the server the cursor was on.
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }
}

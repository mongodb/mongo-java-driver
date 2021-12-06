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

import java.io.Closeable;
import java.util.Iterator;

/**
 * Interface for providing consistent behaviour between different Cursor implementations.
 *
 * @mongodb.driver.manual core/cursors/ Cursors
 * @since 2.12
 */
public interface Cursor extends Iterator<DBObject>, Closeable {

    /**
     * Gets the number of results available locally without blocking, which may be 0, or 0 when the cursor is exhausted or closed.
     *
     * @return the number of results available locally without blocking
     * @since 4.5
     */
    int available();

    /**
     * Gets the server's identifier for this Cursor.
     *
     * @return the cursor's ID, or 0 if there is no active cursor.
     */
    long getCursorId();

    /**
     * Gets the address of the server that data is pulled from. Note that this information may not be available until hasNext() or
     * next() is called.
     *
     * @return the address of the server that data is pulled from
     */
    ServerAddress getServerAddress();

    /**
     * Terminates this cursor on the server.
     */
    void close();
}

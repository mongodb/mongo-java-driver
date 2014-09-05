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


import com.mongodb.annotations.NotThreadSafe;

import java.io.Closeable;
import java.util.Iterator;

/**
 * The Mongo Cursor interface implementing the iterator protocol
 *
 * @since 3.0
 * @param <T> The type of documents the cursor contains
 */
@NotThreadSafe
public interface MongoCursor<T> extends Iterator<T>, Closeable {
    @Override
    void close();

    @Override
    boolean hasNext();

    @Override
    T next();

    /**
     * A special {@code hasNext()} case for tailable cursors providing a non blocking check to see if there are any results available.
     * Returns true if there are more elements available and false if currently there are no more results available.
     *
     * @return {@code true} if the iteration has more elements readily available.
     */
    boolean tryHasNext();

    /**
     * Returns the server cursor
     *
     * @return ServerCursor
     */
    ServerCursor getServerCursor();

    /**
     * Returns the server address
     *
     * @return ServerAddress
     */
    ServerAddress getServerAddress();
}

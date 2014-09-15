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

/**
 * The Mongo Tailable Cursor interface extending Mongo Cursors and adding support for handling tailable cursors.
 *
 * @since 3.0
 * @param <T> The type of documents the cursor contains
 */
@NotThreadSafe
public interface MongoTailableCursor<T> extends MongoCursor<T> {

    /**
     * A special {@code next()} case for tailable cursors providing a non blocking check to see if there are any results available.
     * Returns true if there are more elements available and false if currently there are no more results available.
     *
     * @return {@code true} if the iteration has more elements readily available.
     */
    T tryNext();

}

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

package com.mongodb.client.model;

/**
 * The options to apply to an operation that inserts multiple documents into a collection.
 *
 * @since 3.0
 * @mongodb.driver.manual tutorial/insert-documents/ Insert
 */
public final class InsertManyOptions {
    private boolean ordered = true;

    /**
     * Gets whether the documents should be inserted in the order provided, stopping on the first failed insertion. The default is true.
     * If false, the server will attempt to insert all the documents regardless of an failures.
     *
     * @return whether the the documents should be inserted in order
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Sets whether the server should insert the documents in the order provided.
     *
     * @param ordered true if documents should be inserted in order
     * @return this
     */
    public InsertManyOptions ordered(final boolean ordered) {
        this.ordered = ordered;
        return this;
    }
}

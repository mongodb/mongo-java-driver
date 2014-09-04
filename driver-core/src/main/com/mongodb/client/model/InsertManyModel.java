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

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing an insert of multiple documents.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/insert-documents/ Insert
 */
public final class InsertManyModel<T> {
    private final List<? extends T> documents;
    private boolean ordered;

    /**
     * Construct a new instance.
     *
     * @param documents a non-null, non-empty list of documents to insert
     */
    public InsertManyModel(final List<? extends T> documents) {
        this.documents = notNull("documents", documents);
        isTrueArgument("documents list is not empty", !documents.isEmpty());
    }

    /**
     *
     * @return the list of documents to insert
     */
    public List<? extends T> getDocuments() {
        return documents;
    }

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
    public InsertManyModel<T> ordered(final boolean ordered) {
        this.ordered = ordered;
        return this;
    }
}

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
 * @param <T> the type of document to insert. This can be of any type for which a {@code Codec} is registered
 */
public final class InsertManyModel<T> {
    private final List<? extends T> documents;
    private final InsertManyOptions options;

    /**
     * Construct a new instance.
     *
     * @param documents a non-null, non-empty list of documents to insert
     */
    public InsertManyModel(final List<? extends T> documents) {
        this(documents, new InsertManyOptions());
    }

    /**
     * Construct a new instance.
     *
     * @param documents a non-null, non-empty list of documents to insert
     * @param options the non-null options
     */
    public InsertManyModel(final List<? extends T> documents, final InsertManyOptions options) {
        isTrueArgument("documents list is not empty", !documents.isEmpty());
        this.documents = notNull("documents", documents);
        this.options = notNull("options", options);
    }

    /**
     *
     * @return the list of documents to insert
     */
    public List<? extends T> getDocuments() {
        return documents;
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public InsertManyOptions getOptions() {
        return options;
    }
}

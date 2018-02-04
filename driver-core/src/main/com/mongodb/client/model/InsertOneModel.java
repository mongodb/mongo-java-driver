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

package com.mongodb.client.model;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing an insert of a single document.
 *
 * @since 3.0
 * @mongodb.driver.manual tutorial/insert-documents/ Insert
 * @param <T> the type of document to insert. This can be of any type for which a {@code Codec} is registered
 */
public final class InsertOneModel<T> extends WriteModel<T> {
    private final T document;

    /**
     * Construct a new instance.
     *
     * @param document the document to insert, which may not be null.
     */
    public InsertOneModel(final T document) {
        this.document = notNull("document", document);
    }

    /**
     * Gets the document to insert.
     *
     * @return the document to insert
     */
    public T getDocument() {
        return document;
    }

    @Override
    public String toString() {
        return "InsertOneModel{"
                + "document=" + document
                + '}';
    }
}

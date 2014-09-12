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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing the replacement of at most one document that matches the query filter.
 *
 * @param <T> the type of document to replace. This can be of any type for which a {@code Codec} is registered
 * @param <D> the document type. This can be of any type for which a {@code Codec} is registered
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
 * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
 */
public final class ReplaceOneModel<T, D> extends WriteModel<T, D> {
    private final D filter;
    private final T replacement;
    private boolean upsert;

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param replacement the replacement document
     */
    public ReplaceOneModel(final D filter, final T replacement) {
        this.filter = notNull("filter", filter);
        this.replacement = notNull("replacement", replacement);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     */
    public D getFilter() {
        return filter;
    }

    /**
     * Gets the document which will replace the document matching the query filter.
     *
     * @return the replacement document
     */
    public T getReplacement() {
        return replacement;
    }

    /**
     * Returns true if a new document should be inserted if there are no matches to the query filter.  The default is false.
     *
     * @return true if a new document should be inserted if there are no matches to the query filter
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Set to true if a new document should be inserted if there are no matches to the query filter.
     *
     * @param upsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public ReplaceOneModel<T, D> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }
}

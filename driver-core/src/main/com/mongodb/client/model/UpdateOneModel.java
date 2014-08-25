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

import org.mongodb.Document;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing an update to at most one document that matches the query filter. The update to apply must include only update
 * operators.
 *
 * @param <T> the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
 *           other write models
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
 * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
 */
public final class UpdateOneModel<T> extends WriteModel<T> {
    private final Object filter;
    private final Object update;
    private boolean upsert;

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     */
    public UpdateOneModel(final Object filter, final Object update) {
        this.filter = notNull("filter", filter);
        this.update = notNull("update", update);
    }

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators.
     */
    public UpdateOneModel(final Document filter, final Document update) {
        this((Object) filter, update);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     */
    public Object getFilter() {
        return filter;
    }

    /**
     * Gets the document specifying the updates to apply to the matching document.  The update to apply must include only update
     * operators.
     *
     * @return the document specifying the updates to apply
     */
    public Object getUpdate() {
        return update;
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
    public UpdateOneModel<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }
}

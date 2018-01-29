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

import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing an update to at most one document that matches the query filter. The update to apply must include only update
 * operators.
 *
 * @param <T> the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the other
 *            write models
 * @mongodb.driver.manual tutorial/modify-documents/ Updates
 * @mongodb.driver.manual reference/operator/update/ Update Operators
 * @since 3.0
 */
public final class UpdateOneModel<T> extends WriteModel<T> {
    private final Bson filter;
    private final Bson update;
    private final UpdateOptions options;

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     */
    public UpdateOneModel(final Bson filter, final Bson update) {
        this(filter, update, new UpdateOptions());
    }

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply
     */
    public UpdateOneModel(final Bson filter, final Bson update, final UpdateOptions options) {
        this.filter = notNull("filter", filter);
        this.update = notNull("update", update);
        this.options = notNull("options", options);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     */
    public Bson getFilter() {
        return filter;
    }

    /**
     * Gets the document specifying the updates to apply to the matching document.  The update to apply must include only update operators.
     *
     * @return the document specifying the updates to apply
     */
    public Bson getUpdate() {
        return update;
    }

    /**
     * Gets the options to apply.
     *
     * @return the options
     */
    public UpdateOptions getOptions() {
        return options;
    }
}

/*
 * Copyright 2014-2015 MongoDB, Inc.
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
 * A model describing the removal of at most one document matching the query filter.
 *
 * @param <T> the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
 *           other write models
 * @since 3.0
 * @mongodb.driver.manual tutorial/remove-documents/ Remove
 */
public class DeleteOneModel<T> extends WriteModel<T> {
    private final Bson filter;
    private final DeleteOptions options;

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null.
     */
    public DeleteOneModel(final Bson filter) {
        this(filter, new DeleteOptions());
    }

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param options the options to apply
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public DeleteOneModel(final Bson filter, final DeleteOptions options) {
        this.filter = notNull("filter", filter);
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
     * Gets the options to apply.
     *
     * @return the options
     * @since 3.4
     */
    public DeleteOptions getOptions() {
        return options;
    }
}

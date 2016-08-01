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

import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing the replacement of at most one document that matches the query filter.
 *
 * @param <T> the type of document to replace. This can be of any type for which a {@code Codec} is registered
 * @since 3.0
 * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
 */
public final class ReplaceOneModel<T> extends WriteModel<T> {
    private final Bson filter;
    private final Collation collation;
    private final boolean hasSetCollation;
    private final T replacement;
    private final UpdateOptions options;

    /**
     * Construct a new instance.
     *
     * @param filter    a document describing the query filter, which may not be null.
     * @param replacement the replacement document
     */
    public ReplaceOneModel(final Bson filter, final T replacement) {
        this(filter, replacement, new UpdateOptions());
    }

    /**
     * Construct a new instance.
     *
     * @param filter    a document describing the query filter, which may not be null.
     * @param replacement the replacement document
     * @param options     the options to apply
     */
    public ReplaceOneModel(final Bson filter, final T replacement, final UpdateOptions options) {
        this(filter, null, replacement, options, false);
    }

    /**
     * Construct a new instance.
     *
     * @param filter    a document describing the query filter, which may not be null.
     * @param collation the collation to apply to the filter.
     *                  A null value will use the default {@link Collation} as configured on the server.
     * @param replacement the replacement document
     * @param options     the options to apply
     */
    public ReplaceOneModel(final Bson filter, final Collation collation, final T replacement, final UpdateOptions options) {
        this(filter, collation, replacement, options, true);
    }

    private ReplaceOneModel(final Bson filter, final Collation collation, final T replacement, final UpdateOptions options,
                            final boolean hasSetCollation) {
        this.filter = notNull("filter", filter);
        this.collation = collation;
        this.options = notNull("options", options);
        this.replacement = notNull("replacement", replacement);
        this.hasSetCollation = hasSetCollation;
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
     * Gets the collation to apply to the query, which may be null
     *
     * @return the collation to apply to the query
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Returns true if the collation has been set
     *
     * @return true if the collation has been set
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public boolean hasSetCollation() {
        return hasSetCollation;
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
     * Gets the options to apply.
     *
     * @return the options
     */
    public UpdateOptions getOptions() {
        return options;
    }
}

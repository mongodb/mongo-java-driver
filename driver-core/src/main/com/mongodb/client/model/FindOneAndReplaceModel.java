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
 *
 * @param <T> The replacement type for the command. This can be of any type for which a {@code Codec} is registered
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/findAndModify/
 */
public class FindOneAndReplaceModel<T> {
    private final Object criteria;
    private final T replacement;
    private Object projection;
    private Object sort;
    private boolean upsert;
    private boolean returnReplaced;

    /**
     * Construct a new instance
     *
     * @param criteria the query criteria. This can be of any type for which a {@code Codec} is registered.
     * @param replacement the replacement. This can be of any type for which a {@code Codec} is registered.
     * @mongodb.driver.manual manual/reference/command/findAndModify/
     */
    public FindOneAndReplaceModel(final Object criteria, final T replacement) {
        this.criteria = notNull("criteria", criteria);
        this.replacement = notNull("replacement", replacement);
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     */
    public Object getCriteria() {
        return criteria;
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
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual manual/tutorial/project-fields-from-query-results Projection
     */
    public Object getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null. This can be of any type for which a {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/tutorial/project-fields-from-query-results Projection
     */
    public FindOneAndReplaceModel<T> projection(final Object projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public Object getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null. This can be of any type for which a {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public FindOneAndReplaceModel<T> sort(final Object sort) {
        this.sort = sort;
        return this;
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
    public FindOneAndReplaceModel<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * When true, returns the updated document rather than the original. The default is false.
     *
     * @return true if the updated document should be returned, otherwise false
     */
    public boolean getReturnReplaced() {
        return returnReplaced;
    }

    /**
     * Set true to return the updated document rather than the original.
     *
     * @param returnReplaced set true to return the updated document rather than the original.
     * @return this
     */
    public FindOneAndReplaceModel<T> returnReplaced(final boolean returnReplaced) {
        this.returnReplaced = returnReplaced;
        return this;
    }
}


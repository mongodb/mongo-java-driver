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
 * Finds a single document and updates it.
 *
 * @param <D> the document type. This can be of any type for which a {@code Codec} is registered
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/findAndModify/
 */
public class FindOneAndUpdateModel<D> {
    private final D criteria;
    private final D update;
    private D projection;
    private D sort;
    private boolean upsert;
    private boolean returnUpdated;

    /**
     * Construct a new instance
     *
     * @param criteria the query criteria. This can be of any type for which a {@code Codec} is registered.
     * @param update the update operation. This can be of any type for which a {@code Codec} is registered.
     * @mongodb.driver.manual manual/reference/command/findAndModify/
     */
    public FindOneAndUpdateModel(final D criteria, final D update) {
        this.criteria = notNull("criteria", criteria);
        this.update = notNull("update", update);
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     */
    public D getCriteria() {
        return criteria;
    }

    /**
     * Gets the document specifying the updates to apply to the matching document.  The update to apply must include only update
     * operators.
     *
     * @return the document specifying the updates to apply
     */
    public D getUpdate() {
        return update;
    }

     /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual manual/tutorial/project-fields-from-query-results Projection
     */
    public D getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null. This can be of any type for which a
     * {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/tutorial/project-fields-from-query-results Projection
     */
    public FindOneAndUpdateModel<D> projection(final D projection) {
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
    public D getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null. This can be of any type for which a
     * {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public FindOneAndUpdateModel<D> sort(final D sort) {
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
    public FindOneAndUpdateModel<D> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * When true, returns the updated document rather than the original. The default is false.
     *
     * @return true if the updated document should be returned, otherwise false
     */
    public boolean getReturnUpdated() {
        return returnUpdated;
    }

    /**
     * Set true to return the updated document rather than the original.
     *
     * @param returnUpdated set true to return the updated document rather than the original.
     * @return this
     */
    public FindOneAndUpdateModel<D> returnUpdated(final boolean returnUpdated) {
        this.returnUpdated = returnUpdated;
        return this;
    }
}

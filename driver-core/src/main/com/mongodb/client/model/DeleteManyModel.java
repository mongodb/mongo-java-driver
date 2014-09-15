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
 * A model describing the removal of all documents matching the query criteria.
 *
 * @param <T> the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
 *           other write models
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/remove-documents/ Remove
 */
public final class DeleteManyModel<T> extends WriteModel<T> {
    private final Object criteria;

    /**
     * Construct a new instance.
     *
     * @param criteria a document describing the query criteria, which may not be null. The criteria can be of any type for which a
     * {@code Codec} is registered
     */
    public DeleteManyModel(final Object criteria) {
        this.criteria = notNull("criteria", criteria);
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     */
    public Object getCriteria() {
        return criteria;
    }
}

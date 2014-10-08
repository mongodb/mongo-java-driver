/*
 * Copyright (c) 2008-2014 MongoObjectB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONObjectITIONS OF ANY KINObject, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model;

/**
 * A model describing the removal of at most one document matching the query criteria.
 *
 * @param <T> the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
 *           other write models
 * @since 3.0
 * @mongodb.driver.manual tutorial/remove-documents/ Remove
 */
public class DeleteOneModel<T> extends WriteModel<T> {
    private final Object criteria;

    /**
     * Construct a new instance.
     *
     * @param criteria a document describing the query criteria, which may not be null. The criteria can be of any type for which a
     * {@code Codec} is registered
     */
    public DeleteOneModel(final Object criteria) {
        this.criteria = criteria;
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

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

package com.mongodb.operation;

import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A base class for a representation of an update.
 *
 * @since 3.0
 */
public abstract class BaseUpdateRequest extends WriteRequest {
    private final BsonDocument criteria;
    private boolean isUpsert = false;

    BaseUpdateRequest(final BsonDocument criteria) {
        super();
        this.criteria = notNull("criteria", criteria);
    }

    /**
     * Gets the query criteria for the update.
     *
     * @return the criteria
     */
    public BsonDocument getCriteria() {
        return criteria;
    }

    /**
     * Gets whether this update will insert a new document if no documents match the criteria.  The default is false.
     * @return whether this update will insert a new document if no documents match the criteria
     */
    public boolean isUpsert() {
        return isUpsert;
    }

    /**
     * Sets whether this update will insert a new document if no documents match the criteria.
     * @param isUpsert whether this update will insert a new document if no documents match the criteria
     * @return this
     */
    public BaseUpdateRequest upsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    /**
     * Gets whether this update will update all documents matching the criteria.  The default is update-dependent.
     * @return whether this update will update all documents matching the criteria
     */
    public abstract boolean isMulti();
}

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

package com.mongodb.bulk;

import com.mongodb.client.model.Collation;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An update to one or more documents.
 *
 * @since 3.0
 */
public final class UpdateRequest extends WriteRequest {
    private final BsonDocument update;
    private final Type updateType;
    private final BsonDocument filter;
    private boolean isMulti = true;
    private boolean isUpsert = false;
    private Collation collation;

    /**
     * Construct a new instance.
     * @param filter the non-null query filter
     * @param update the non-null update operations
     * @param updateType the update type, which must be either UPDATE or REPLACE
     */
    public UpdateRequest(final BsonDocument filter, final BsonDocument update, final Type updateType) {
        if (updateType != Type.UPDATE && updateType != Type.REPLACE) {
            throw new IllegalArgumentException("Update type must be UPDATE or REPLACE");
        }

        this.filter = notNull("filter", filter);
        this.update = notNull("update", update);
        this.updateType = updateType;
        this.isMulti = updateType == Type.UPDATE;
    }

    @Override
    public Type getType() {
        return updateType;
    }

    /**
     * Gets the query filter for the update.
     *
     * @return the filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Gets the update.
     *
     * @return the update
     */
    public BsonDocument getUpdate() {
        return update;
    }

    /**
     * Gets whether this update will update all documents matching the filter.  The default is true.
     *
     * @return whether this update will update all documents matching the filter
     */
    public boolean isMulti() {
        return isMulti;
    }

    /**
     * Sets whether this will update all documents matching the query filter.
     *
     * @param isMulti whether this will update all documents matching the query filter
     * @return this
     */
    public UpdateRequest multi(final boolean isMulti) {
        if (isMulti && updateType == Type.REPLACE) {
            throw new IllegalArgumentException("Replacements can not be multi");
        }
        this.isMulti = isMulti;
        return this;
    }

    /**
     * Gets whether this update will insert a new document if no documents match the filter.  The default is false.
     * @return whether this update will insert a new document if no documents match the filter
     */
    public boolean isUpsert() {
        return isUpsert;
    }

    /**
     * Sets whether this update will insert a new document if no documents match the filter.
     * @param isUpsert whether this update will insert a new document if no documents match the filter
     * @return this
     */
    public UpdateRequest upsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * @param collation the collation options
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public UpdateRequest collation(final Collation collation) {
        this.collation = collation;
        return this;
    }
}


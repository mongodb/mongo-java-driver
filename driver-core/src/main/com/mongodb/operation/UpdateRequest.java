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
 * An update to one or more documents.
 *
 * @since 3.0
 */
public final class UpdateRequest extends BaseUpdateRequest {
    private final BsonDocument update;
    private final Type updateType;
    private boolean isMulti = true;

    /**
     * Construct a new instance.
     *  @param criteria the non-null query criteria
     * @param update the non-null update operations
     * @param updateType the update type, which must be either UPDATE or REPLACE
     */
    public UpdateRequest(final BsonDocument criteria, final BsonDocument update, final Type updateType) {
        super(criteria);
        this.update = notNull("update", update);
        if (updateType != Type.UPDATE && updateType != Type.REPLACE) {
            throw new IllegalArgumentException("Update type must be UPDATE or REPLACE");
        }
        this.updateType = updateType;
        this.isMulti = updateType == Type.UPDATE;
    }

    /**
     * Gets the update.
     *
     * @return the update operations
     */
    public BsonDocument getUpdate() {
        return update;
    }

    /**
     * Gets whether this update will update all documents matching the criteria.  The default is true.
     *
     * @return whether this update will update all documents matching the criteria
     */
    public boolean isMulti() {
        return isMulti;
    }

    /**
     * Sets whether this will update all documents matching the query criteria.
     *
     * @param isMulti whether this will update all documents matching the query criteria
     * @return this
     */
    public UpdateRequest multi(final boolean isMulti) {
        if (isMulti && updateType == Type.REPLACE) {
            throw new IllegalArgumentException("Replacements can not be multi");
        }
        this.isMulti = isMulti;
        return this;
    }

    @Override
    public UpdateRequest upsert(final boolean isUpsert) {
        super.upsert(isUpsert);
        return this;
    }

    @Override
    public Type getType() {
        return updateType;
    }

}


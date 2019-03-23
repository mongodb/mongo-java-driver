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

package com.mongodb.internal.bulk;

import com.mongodb.client.model.Collation;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a delete.
 *
 * @since 3.0
 */
@Deprecated
public final class DeleteRequest extends WriteRequest {
    private final BsonDocument filter;
    private boolean isMulti = true;
    private Collation collation;

    /**
     * Construct a new instance.
     *
     * @param filter the non-null query filter
     */
    public DeleteRequest(final BsonDocument filter) {
        super();
        this.filter = notNull("filter", filter);
    }

    /**
     * Gets the query Object filter.
     *
     * @return the Object filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets whether all documents matching the query filter will be removed.
     *
     * @param isMulti true if all documents matching the query filter will be removed
     * @return this
     */
    public DeleteRequest multi(final boolean isMulti) {
        this.isMulti = isMulti;
        return this;
    }

    /**
     * Gets whether all documents matching the query filter will be removed.  The default is true.
     *
     * @return whether all documents matching the query filter will be removed
     */
    public boolean isMulti() {
        return isMulti;
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
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public DeleteRequest collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Type getType() {
        return Type.DELETE;
    }
}

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

package com.mongodb;

import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.codecs.Encoder;

import java.util.List;

/**
 * A builder for a single update request.
 *
 * @mongodb.driver.manual /reference/command/update
 * @since 2.12
 */
public class BulkUpdateRequestBuilder {
    private final BulkWriteOperation bulkWriteOperation;
    private final DBObject query;
    private final boolean upsert;
    private final Encoder<DBObject> queryCodec;
    private final Encoder<DBObject> replacementCodec;
    private Collation collation;
    private final List<? extends DBObject> arrayFilters;

    BulkUpdateRequestBuilder(final BulkWriteOperation bulkWriteOperation, final DBObject query, final boolean upsert,
                             final Encoder<DBObject> queryCodec, final Encoder<DBObject> replacementCodec,
                             @Nullable final Collation collation, @Nullable final List<? extends DBObject> arrayFilters) {
        this.bulkWriteOperation = bulkWriteOperation;
        this.query = query;
        this.upsert = upsert;
        this.queryCodec = queryCodec;
        this.replacementCodec = replacementCodec;
        this.collation = collation;
        this.arrayFilters = arrayFilters;
    }

    /**
     * Returns the collation
     *
     * @return the collation
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public BulkUpdateRequestBuilder collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the array filters to apply to the update operation
     * @return the array filters, which may be null
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    public List<? extends DBObject> getArrayFilters() {
        return arrayFilters;
    }

    /**
     * Adds a request to replace one document in the collection that matches the query with which this builder was created.
     *
     * @param document the replacement document, which must be structured just as a document you would insert.  It can not contain any
     *                 update operators.
     */
    public void replaceOne(final DBObject document) {
        bulkWriteOperation.addRequest(new ReplaceRequest(query, document, upsert, queryCodec, replacementCodec, collation));
    }

    /**
     * Adds a request to update all documents in the collection that match the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void update(final DBObject update) {
        bulkWriteOperation.addRequest(new UpdateRequest(query, update, true, upsert, queryCodec, collation, arrayFilters));
    }

    /**
     * Adds a request to update one document in the collection that matches the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void updateOne(final DBObject update) {
        bulkWriteOperation.addRequest(new UpdateRequest(query, update, false, upsert, queryCodec, collation, arrayFilters));
    }
}

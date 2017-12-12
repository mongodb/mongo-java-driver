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

package com.mongodb;

import com.mongodb.client.model.Collation;
import org.bson.codecs.Encoder;

import java.util.List;

/**
 * A builder for a single write request.
 *
 * @mongodb.server.release 2.6
 * @mongodb.driver.manual /reference/command/delete/ Delete
 * @mongodb.driver.manual /reference/command/update/ Update
 * @mongodb.driver.manual /reference/command/insert/ Insert
 * @since 2.12
 */
public class BulkWriteRequestBuilder {
    private final BulkWriteOperation bulkWriteOperation;
    private final DBObject query;
    private final Encoder<DBObject> codec;
    private final Encoder<DBObject> replacementCodec;
    private Collation collation;

    BulkWriteRequestBuilder(final BulkWriteOperation bulkWriteOperation, final DBObject query, final Encoder<DBObject> queryCodec,
                            final Encoder<DBObject> replacementCodec) {
        this.bulkWriteOperation = bulkWriteOperation;
        this.query = query;
        this.codec = queryCodec;
        this.replacementCodec = replacementCodec;
    }

    /**
     * Returns the collation
     *
     * @return the collation
     * @since 3.4
     * @mongodb.server.release 3.4
     */
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
    public BulkWriteRequestBuilder collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Adds a request to remove all documents in the collection that match the query with which this builder was created.
     */
    public void remove() {
        bulkWriteOperation.addRequest(new RemoveRequest(query, true, codec, collation));
    }

    /**
     * Adds a request to remove one document in the collection that matches the query with which this builder was created.
     */
    public void removeOne() {
        bulkWriteOperation.addRequest(new RemoveRequest(query, false, codec, collation));
    }

    /**
     * Adds a request to replace one document in the collection that matches the query with which this builder was created.
     *
     * @param document the replacement document, which must be structured just as a document you would insert.  It can not contain any
     *                 update operators.
     */
    public void replaceOne(final DBObject document) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec, collation, null).replaceOne(document);
    }

    /**
     * Adds a request to update all documents in the collection that match the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void update(final DBObject update) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec, collation, null).update(update);
    }

    /**
     * Adds a request to update one document in the collection that matches the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void updateOne(final DBObject update) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec, collation, null).updateOne(update);
    }

    /**
     * Specifies that the request being built should be an upsert.
     *
     * @return a new builder that allows only update and replace, since upsert does not apply to remove.
     * @mongodb.driver.manual tutorial/modify-documents/#upsert-option Upsert
     */
    public BulkUpdateRequestBuilder upsert() {
        return new BulkUpdateRequestBuilder(bulkWriteOperation, query, true, codec, replacementCodec, collation, null);
    }

    /**
     * Specifies that the request being built should use the given array filters for an update.  Note that this option only applies to
     * update operations and will be ignored for replace operations
     *
     * @param arrayFilters the array filters to apply to the update operation
     * @return a new builder that allows only update and replace, since upsert does not apply to remove.
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public BulkUpdateRequestBuilder arrayFilters(final List<? extends DBObject> arrayFilters) {
        return new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec, collation, arrayFilters);
    }
}

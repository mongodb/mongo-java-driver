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

/**
 * A builder for a single update request.
 *
 * @since 2.12
 */
public class BulkUpdateRequestBuilder {
    private final BulkWriteOperation bulkWriteOperation;
    private final DBObject query;
    private final boolean upsert;
    private final DBObjectCodec codec;

    BulkUpdateRequestBuilder(final BulkWriteOperation bulkWriteOperation, final DBObject query, final boolean upsert,
                             final DBObjectCodec codec) {
        this.bulkWriteOperation = bulkWriteOperation;
        this.query = query;
        this.upsert = upsert;
        this.codec = codec;
    }

    /**
     * Adds a request to replace one document in the collection that matches the query with which this builder was created.
     *
     * @param document the replacement document, which must be structured just as a document you would insert.  It can not contain any
     *                 update operators.
     */
    public void replaceOne(final DBObject document) {
        bulkWriteOperation.addRequest(new ReplaceRequest(query, document, upsert, codec));
    }

    /**
     * Adds a request to update all documents in the collection that match the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void update(final DBObject update) {
        bulkWriteOperation.addRequest(new UpdateRequest(query, update, true, upsert, codec));
    }

    /**
     * Adds a request to update one document in the collection that matches the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void updateOne(final DBObject update) {
        bulkWriteOperation.addRequest(new UpdateRequest(query, update, false, upsert, codec));
    }
}

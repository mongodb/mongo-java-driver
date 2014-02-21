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
 * A builder for a single write request.
 *
 * @since 2.12
 */
public class BulkWriteRequestBuilder {
    private final BulkWriteOperation bulkWriteOperation;
    private final DBObject query;

    BulkWriteRequestBuilder(final BulkWriteOperation bulkWriteOperation, final DBObject query) {
        this.bulkWriteOperation = bulkWriteOperation;
        this.query = query;
    }

    /**
     * Adds a request to remove all matching documents to the bulk operation.
     */
    public void remove() {
        bulkWriteOperation.addRequest(new RemoveRequest(query, true));
    }

    /**
     * Adds a request to remove one matching documents to the bulk operation.
     */
    public void removeOne() {
        bulkWriteOperation.addRequest(new RemoveRequest(query, false));
    }

    /**
     * Adds a request to replace one matching documents to the bulk operation.
     *
     * @param document the replacement document
     */
    public void replaceOne(final DBObject document) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false).replaceOne(document);
    }

    /**
     * Adds a request to replace one matching documents to the bulk operation.
     *
     * @param update the update criteria
     */
    public void update(final DBObject update) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false).update(update);
    }

    /**
     * Adds a request to update one matching documents to the bulk operation.
     *
     * @param update the update criteria
     */
    public void updateOne(final DBObject update) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false).updateOne(update);
    }

    /**
     * Specifies that the request being built should be an upsert.
     *
     * @return a new builder that allows only update and replace, since upsert does not apply to remove.
     *
     * @mongodb.driver.manual reference/method/db.collection.update/#upsert-parameter Upsert
     */
    public BulkUpdateRequestBuilder upsert() {
        return new BulkUpdateRequestBuilder(bulkWriteOperation, query, true);
    }
}

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

import org.bson.codecs.Encoder;

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

    BulkWriteRequestBuilder(final BulkWriteOperation bulkWriteOperation, final DBObject query, final Encoder<DBObject> queryCodec,
                            final Encoder<DBObject> replacementCodec) {
        this.bulkWriteOperation = bulkWriteOperation;
        this.query = query;
        this.codec = queryCodec;
        this.replacementCodec = replacementCodec;
    }

    /**
     * Adds a request to remove all documents in the collection that match the query with which this builder was created.
     */
    public void remove() {
        bulkWriteOperation.addRequest(new RemoveRequest(query, true, codec));
    }

    /**
     * Adds a request to remove one document in the collection that matches the query with which this builder was created.
     */
    public void removeOne() {
        bulkWriteOperation.addRequest(new RemoveRequest(query, false, codec));
    }

    /**
     * Adds a request to replace one document in the collection that matches the query with which this builder was created.
     *
     * @param document the replacement document, which must be structured just as a document you would insert.  It can not contain any
     *                 update operators.
     */
    public void replaceOne(final DBObject document) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec).replaceOne(document);
    }

    /**
     * Adds a request to update all documents in the collection that match the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void update(final DBObject update) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec).update(update);
    }

    /**
     * Adds a request to update one document in the collection that matches the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void updateOne(final DBObject update) {
        new BulkUpdateRequestBuilder(bulkWriteOperation, query, false, codec, replacementCodec).updateOne(update);
    }

    /**
     * Specifies that the request being built should be an upsert.
     *
     * @return a new builder that allows only update and replace, since upsert does not apply to remove.
     * @mongodb.driver.manual tutorial/modify-documents/#upsert-option Upsert
     */
    public BulkUpdateRequestBuilder upsert() {
        return new BulkUpdateRequestBuilder(bulkWriteOperation, query, true, codec, replacementCodec);
    }
}

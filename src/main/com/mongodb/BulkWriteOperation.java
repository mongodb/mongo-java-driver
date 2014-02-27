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

import java.util.ArrayList;
import java.util.List;

import static org.bson.util.Assertions.isTrue;

/**
 * A bulk write operation.
 *
 * @since 2.12
 */
public class BulkWriteOperation {
    private final boolean ordered;
    private final DBCollection collection;
    private final List<WriteRequest> requests = new ArrayList<WriteRequest>();
    private boolean closed;

    BulkWriteOperation(final boolean ordered, final DBCollection collection) {
        this.ordered = ordered;
        this.collection = collection;
    }

    /**
     * Returns true if this is building an ordered bulk write request.
     *
     * @return whether this is building an ordered bulk write operation
     *
     * @see DBCollection#initializeOrderedBulkOperation()
     * @see DBCollection#initializeUnorderedBulkOperation()
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Add an insert request to the bulk operation
     *
     * @param document the document to insert
     */
    public void insert(final DBObject document) {
        isTrue("already executed", !closed);
        addRequest(new InsertRequest(document));
    }

    /**
     * Start building a write request to add to the bulk write operation.
     *
     * @param query the query for an update, replace or remove request
     * @return a builder for a single write request
     */
    public BulkWriteRequestBuilder find(final DBObject query) {
        isTrue("already executed", !closed);
        return new BulkWriteRequestBuilder(this, query);
    }

    /**
     * Execute the bulk write operation.
     *
     * @return the result of the bulk write operation.
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     */
    public BulkWriteResult execute() {
        isTrue("already executed", !closed);

        closed = true;
        return collection.executeBulkWriteOperation(ordered, requests);
    }

    /**
     * Execute the bulk write operation with the given write concern.
     *
     * @param writeConcern the write concern to apply to the bulk operation
     *
     * @return the result of the bulk write operation.
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     */
    public BulkWriteResult execute(final WriteConcern writeConcern) {
        isTrue("already executed", !closed);

        closed = true;
        return collection.executeBulkWriteOperation(ordered, requests, writeConcern);
    }

    void addRequest(final WriteRequest request) {
        isTrue("already executed", !closed);
        requests.add(request);
    }


}

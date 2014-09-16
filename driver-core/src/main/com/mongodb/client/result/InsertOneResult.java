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

package com.mongodb.client.result;

import org.bson.BsonValue;

/**
 * The result of successfully inserting a document into a MongoDB collection.
 *
 * @since 3.0
 */
public final class InsertOneResult {
    private final BsonValue insertedId;
    private final int insertedCount;

    public InsertOneResult(final BsonValue insertedId, final int insertedCount) {
        this.insertedId = insertedId;
        this.insertedCount = insertedCount;
    }

    /**
     * The identifier of the document that was inserted. If the server generated the identifier, this value
     * will be null as the driver does not have access to that data.
     *
     * @return the inserted id
     */
    public BsonValue getInsertedId() {
        return insertedId;
    }

    // TODO: when is this not 1?
    public int getInsertedCount() {
        return insertedCount;
    }
}

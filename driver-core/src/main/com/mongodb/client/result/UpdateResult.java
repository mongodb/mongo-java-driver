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
 * The result of a replace operation.
 *
 * @since 3.0
 */
public class UpdateResult {
    private final long matchedCount;
    private final long modifiedCount;
    private final BsonValue upsertedId;

    /**
     * Construct an instance
     *
     * @param matchedCount  the number of documents matched
     * @param modifiedCount the number of documents modified
     * @param upsertedId    if the replace resulted in an inserted document, the id of the inserted document
     */
    public UpdateResult(final long matchedCount, final long modifiedCount, final BsonValue upsertedId) {
        this.matchedCount = matchedCount;
        this.modifiedCount = modifiedCount;
        this.upsertedId = upsertedId;
    }

    /**
     * Gets the number of documents matched by the query.
     *
     * @return the number of documents matched
     */
    public long getMatchedCount() {
        return matchedCount;
    }

    /**
     * Gets he number of documents modified by the update.
     *
     * @return the number of documents modified
     */
    public long getModifiedCount() {
        return modifiedCount;
    }

    /**
     * If the replace resulted in an inserted document, gets the _id of the inserted document, otherwise null.
     *
     * @return if the replace resulted in an inserted document, the _id of the inserted document, otherwise null
     */
    public BsonValue getUpsertedId() {
        return upsertedId;
    }
}

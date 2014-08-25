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

// TODO: this is identical to ReplaceOneResult
public final class UpdateResult {
    private final long matchedCount;
    private final long modifiedCount;
    private final BsonValue upsertedId;

    public UpdateResult(final long matchedCount, final long modifiedCount, final BsonValue upsertedId) {
        this.matchedCount = matchedCount;
        this.modifiedCount = modifiedCount;
        this.upsertedId = upsertedId;
    }

    public long getMatchedCount() {
        return matchedCount;
    }

    public long getModifiedCount() {
        return modifiedCount;
    }

    // TODO: BsonValue or Object?
    public BsonValue getUpsertedId() {
        return upsertedId;
    }
}

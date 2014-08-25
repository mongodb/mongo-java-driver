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

import java.util.List;

public final class InsertManyResult {

    private final List<BsonValue> insertedIds;
    private final int insertedCount;

    public InsertManyResult(final List<BsonValue> insertedIds, final int insertedCount) {
        this.insertedIds = insertedIds;
        this.insertedCount = insertedCount;
    }

    // TODO: BsonValue or Object?
    public List<BsonValue> getInsertedIds() {
        return insertedIds;
    }

    // TODO: when is this not equal to the size of number of documents inserted
    public int getInsertedCount() {
        return insertedCount;
    }
}

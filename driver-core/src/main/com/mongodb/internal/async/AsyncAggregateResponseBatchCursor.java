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

package com.mongodb.internal.async;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Extends the async batch cursor interface to include information included in an aggregate or getMore response.
 *
 * @param <T> The type of documents the cursor contains
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#wire-op-get-more OP_GET_MORE
 */
public interface AsyncAggregateResponseBatchCursor<T> extends AsyncBatchCursor<T> {
    /**
     * Returns the postBatchResumeToken.
     *
     * @return the postBatchResumeToken
     */
    BsonDocument getPostBatchResumeToken();

    /**
     * Returns the operation time found in the aggregate or getMore response.
     *
     * @return the operation time
     */
    BsonTimestamp getOperationTime();

    /**
     * Returns true if the first batch was empty.
     *
     * @return true if the first batch was empty
     */
    boolean isFirstBatchEmpty();
}

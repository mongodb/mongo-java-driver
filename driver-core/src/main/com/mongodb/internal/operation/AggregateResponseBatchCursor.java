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

package com.mongodb.internal.operation;

import com.mongodb.annotations.NotThreadSafe;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Extends the batch cursor interface to include information included in an aggregate or getMore response.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public interface AggregateResponseBatchCursor<T> extends BatchCursor<T> {
    BsonDocument getPostBatchResumeToken();

    BsonTimestamp getOperationTime();

    boolean isFirstBatchEmpty();

    int getMaxWireVersion();
}

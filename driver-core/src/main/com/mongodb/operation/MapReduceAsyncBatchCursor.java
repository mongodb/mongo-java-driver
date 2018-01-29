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

package com.mongodb.operation;

import com.mongodb.async.AsyncBatchCursor;

/**
 * Represents the future results of a map-reduce operation as a cursor.  Users can iterate over the results and additionally get relevant
 * statistics about the operation.
 *
 * @param <T> the type of each result, usually some sort of document.
 * @since 3.0
 */
public interface MapReduceAsyncBatchCursor<T> extends AsyncBatchCursor<T> {
    /**
     * Get the statistics for this map-reduce operation
     *
     * @return the statistics
     */
    MapReduceStatistics getStatistics();
}

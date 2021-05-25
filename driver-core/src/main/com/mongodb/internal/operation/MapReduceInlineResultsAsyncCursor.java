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

import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.connection.QueryResult;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
class MapReduceInlineResultsAsyncCursor<T> extends AsyncSingleBatchQueryCursor<T> implements MapReduceAsyncBatchCursor<T> {

    private final MapReduceStatistics statistics;

    MapReduceInlineResultsAsyncCursor(final ClientSideOperationTimeout clientSideOperationTimeout, final QueryResult<T> queryResult,
                                      final MapReduceStatistics statistics) {
        super(clientSideOperationTimeout, queryResult);
        this.statistics = statistics;
    }

    @Override
    public MapReduceStatistics getStatistics() {
        return statistics;
    }
}

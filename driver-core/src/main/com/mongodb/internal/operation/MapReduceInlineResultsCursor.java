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

import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.QueryResult;
import org.bson.codecs.Decoder;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 *
 * @param <T> the operations result type.
 */
class MapReduceInlineResultsCursor<T> extends QueryBatchCursor<T> implements MapReduceBatchCursor<T> {
    private final MapReduceStatistics statistics;

    MapReduceInlineResultsCursor(final QueryResult<T> queryResult, final Decoder<T> decoder, final ConnectionSource connectionSource,
                                 final MapReduceStatistics statistics) {
        super(queryResult, 0, 0, decoder, connectionSource);
        this.statistics = statistics;
    }

    @Override
    public MapReduceStatistics getStatistics() {
        return statistics;
    }
}

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

import java.util.List;

/**
 * Container for the result of aggregation operation.
 *
 * @mongodb.server.release 2.2
 * @mongodb.driver.manual aggregation/ Aggregation
 * @deprecated Replace with use of aggregate methods in {@link DBCollection} that return instances of {@link Cursor}.
 * @see DBCollection#aggregate(List, AggregationOptions)
 */
@Deprecated
public class AggregationOutput {
    private final List<DBObject> results;

    AggregationOutput(final List<DBObject> results) {
        this.results = results;
    }

    /**
     * Returns the results of the aggregation.
     *
     * @return iterable collection of {@link DBObject}
     */
    @SuppressWarnings("unchecked")
    public Iterable<DBObject> results() {
        return results;
    }
}

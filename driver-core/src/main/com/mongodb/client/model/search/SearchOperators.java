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

package com.mongodb.client.model.search;

import org.bson.BsonBoolean;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Builder API for Atlas search.
 *
 * @since 4.4
 * @mongodb.driver.manual.atlas reference/atlas-search/query-syntax/
 */
public interface SearchOperators {

    /**
     * A builder for the {@code text} search operator.
     *
     * @param query the query 
     * @param path the path
     * @return an instance of {code TextSearchOperator} with the given query and path
     * @mongodb.driver.manual.atlas reference/atlas-search/text/
     */
    static TextSearchOperator text(final String query, final SearchPath path) {
        return new TextSearchOperator(singletonList(query), singletonList(path), null, null);
    }

    /**
     * A builder for the {@code text} search operator.
     *
     * @param query the strings to search for. Atlas Search looks for a match for each term in the string separately.
     * @param path the indexed fields to search in
     * @return an instance of {code TextSearchOperator} with the given query and path
     * @mongodb.driver.manual.atlas reference/atlas-search/text/
     */
    static TextSearchOperator text(final List<String> query, final List<SearchPath> path) {
        return new TextSearchOperator(query, path, null, null);
    }

    static EqualsSearchOperator equal(final boolean value, final SearchPath path) {
        return new EqualsSearchOperator(BsonBoolean.valueOf(value), singletonList(path), null);
    }

    static EqualsSearchOperator equal(final boolean value, final List<SearchPath> path) {
        return new EqualsSearchOperator(BsonBoolean.valueOf(value), path, null);
    }

    static EqualsSearchOperator equal(final ObjectId value, final SearchPath path) {
        return new EqualsSearchOperator(new BsonObjectId(value), singletonList(path), null);
    }

    static EqualsSearchOperator equal(final ObjectId value, final List<SearchPath> path) {
        return new EqualsSearchOperator(new BsonObjectId(value), path, null);
    }

    static RangeSearchOperator range(final SearchPath path) {
        return new RangeSearchOperator(singletonList(path), null, null, null, null, null);
    }

    static RangeSearchOperator range(final List<SearchPath> path) {
        return new RangeSearchOperator(path, null, null, null, null, null);
    }

    static AutoCompleteSearchOperator autoComplete(final String query, final SearchPath path) {
        return new AutoCompleteSearchOperator(singletonList(query), singletonList(path), null, null, null, null);

    }

    static AutoCompleteSearchOperator autoComplete(final List<String> query, final List<SearchPath> path) {
        return new AutoCompleteSearchOperator(query, path, null, null, null, null);
    }

    static CompoundSearchOperator compound() {
        return new CompoundSearchOperator(null, null, null, null,null, null);
    }
}

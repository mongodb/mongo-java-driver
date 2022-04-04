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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Evolving;
import com.mongodb.client.model.Aggregates;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

/**
 * This interface represents optional fields of the {@code $search} pipeline stage of an aggregation pipeline.
 *
 * @see Aggregates#search(SearchOperator, SearchOptions)
 * @see Aggregates#search(SearchCollector, SearchOptions)
 * @mongodb.atlas.manual atlas-search/query-syntax/#-search $search syntax
 * @since 4.6
 */
@Evolving
public interface SearchOptions extends Bson {
    /**
     * Creates a new {@link SearchOptions} with the index name specified.
     *
     * @param name The name of the index to use.
     * @return A new {@link SearchOptions}.
     */
    SearchOptions index(String name);

    /**
     * Creates a new {@link SearchOptions} with the highlighting options specified.
     *
     * @param option The highlighting option.
     * @return A new {@link SearchOptions}.
     */
    SearchOptions highlight(SearchHighlight option);

    /**
     * Creates a new {@link SearchOptions} with the counting options specified.
     *
     * @param option The counting option.
     * @return A new {@link SearchOptions}.
     */
    @Beta
    SearchOptions count(SearchCount option);

    /**
     * Creates a new {@link SearchOptions} with the specified option in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchOptions} objects,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchOptions options1 = SearchOptions.defaultSearchOptions().index("indexName")
     *  SearchOptions options2 = SearchOptions.defaultSearchOptions().option("index", new BsonString("indexName"))
     * }</pre>
     *
     * @param name A {@link Bson} representing the required {@link SearchOptions}.
     * @param value A {@link Bson} representing the required {@link SearchOptions}.
     * @return A new {@link SearchOptions}.
     */
    SearchOptions option(String name, BsonValue value);

    /**
     * Returns {@link SearchOptions} that represents server defaults.
     *
     * @return {@link SearchOptions} that represents server defaults.
     */
    static SearchOptions defaultSearchOptions() {
        return SearchConstructibleBson.EMPTY;
    }
}

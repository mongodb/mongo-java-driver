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
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ToBsonField;
import org.bson.BsonDocument;

import java.util.Iterator;

import static com.mongodb.internal.client.model.BsonUtil.toBsonDocument;
import static org.bson.assertions.Assertions.notNull;

/**
 * The core part of the {@link Aggregates#search(SearchCollector, SearchOptions) $search}
 * pipeline stage of an aggregation pipeline.
 * {@link SearchCollector}s allow returning metadata together with the matching search results.
 * You may use the {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)}, to extract this metadata.
 *
 * @mongodb.atlas.manual atlas-search/operators-and-collectors/#collectors Search collectors
 * @since 4.6
 */
@Evolving
public interface SearchCollector extends ToBsonField {
    /**
     * Returns a {@link SearchCollector} that groups results by values or ranges in the specified faceted fields and returns the count
     * for each of those groups.
     *
     * @param operator A search operator to use.
     * @param facets Non-empty facets definitions.
     * @return The requested {@link SearchCollector}.
     * @mongodb.atlas.manual atlas-search/facet/ facet collector
     */
    @Beta
    static FacetSearchCollector facet(final SearchOperator operator, final Iterable<SearchFacet> facets) {
        notNull("operator", operator);
        notNull("facets", facets);
        Iterator<SearchFacet> facetIterator = facets.iterator();
        if (!facetIterator.hasNext()) {
            throw new IllegalArgumentException("facets must not be empty");
        }
        return new BsonFieldToManifoldAdapter(new BsonField("facet", new BsonDocument("operator", operator.appendTo(new BsonDocument()))
                .append("facets", toBsonDocument(facetIterator))));
    }

    /**
     * Creates a {@link SearchCollector} from a {@link BsonField} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchCollector}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchCollector collector1 = SearchCollector.facet(
     *          SearchOperator.exists(
     *                  SearchPath.fieldPath("fieldName")),
     *          Collections.singleton(
     *                  SearchFacet.stringFacet(
     *                          "facetName",
     *                          SearchPath.fieldPath("fieldName"))));
     *  SearchCollector collector2 = SearchCollector.of(new BsonField("facet",
     *          new BsonDocument("operator", SearchOperator.exists(
     *                  SearchPath.fieldPath("fieldName")).appendTo(new BsonDocument()))
     *                  .append("facets",
     *                          SearchFacet.stringFacet(
     *                                  "facetName",
     *                                  SearchPath.fieldPath("fieldName")).appendTo(new BsonDocument()))));
     * }</pre>
     *
     * @param collector A {@link BsonField} representing the required {@link SearchCollector}.
     * @return The requested {@link SearchCollector}.
     */
    static SearchCollector of(final BsonField collector) {
        return new BsonFieldToManifoldAdapter(notNull("collector", collector));
    }
}

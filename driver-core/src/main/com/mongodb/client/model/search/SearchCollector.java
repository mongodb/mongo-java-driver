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
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.search.SearchFacet.combineToBson;
import static org.bson.assertions.Assertions.notNull;

/**
 * The core part of the {@link Aggregates#search(SearchCollector, SearchOptions) $search} pipeline stage of an aggregation pipeline.
 * {@link SearchCollector}s allow returning metadata together with the search results.
 * You may use the {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)}, to extract this metadata.
 *
 * @mongodb.atlas.manual atlas-search/operators-and-collectors/#collectors Search collectors
 * @since 4.7
 */
@Evolving
public interface SearchCollector extends Bson {
    /**
     * Returns a {@link SearchCollector} that groups results by values or ranges in the specified faceted fields and returns the count
     * for each of those groups.
     *
     * @param operator The search operator to use.
     * @param facets The non-empty facet definitions.
     * @return The requested {@link SearchCollector}.
     * @mongodb.atlas.manual atlas-search/facet/ facet collector
     */
    @Beta
    static FacetSearchCollector facet(final SearchOperator operator, final Iterable<? extends SearchFacet> facets) {
        notNull("operator", operator);
        notNull("facets", facets);
        return new SearchConstructibleBsonElement("facet", new Document("operator", operator)
                .append("facets", combineToBson(facets)));
    }

    /**
     * Creates a {@link SearchCollector} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchCollector}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchCollector collector1 = SearchCollector.facet(
     *          SearchOperator.exists(
     *                  SearchPath.fieldPath("fieldName")),
     *          Arrays.asList(
     *                  SearchFacet.stringFacet(
     *                          "stringFacetName",
     *                          SearchPath.fieldPath("stringFieldName")),
     *                  SearchFacet.numberFacet(
     *                          "numberFacetName",
     *                          SearchPath.fieldPath("numberFieldName"),
     *                          Arrays.asList(10, 20, 30))));
     *  SearchCollector collector2 = SearchCollector.of(new Document("facet",
     *          new Document("operator", SearchOperator.exists(
     *                  SearchPath.fieldPath("fieldName")))
     *                  .append("facets", SearchFacet.combineToBson(Arrays.asList(
     *                          SearchFacet.stringFacet(
     *                                  "stringFacetName",
     *                                  SearchPath.fieldPath("stringFieldName")),
     *                          SearchFacet.numberFacet(
     *                                  "numberFacetName",
     *                                  SearchPath.fieldPath("numberFieldName"),
     *                                  Arrays.asList(10, 20, 30)))))));
     * }</pre>
     *
     * @param collector A {@link Bson} representing the required {@link SearchCollector}.
     * @return The requested {@link SearchCollector}.
     */
    static SearchCollector of(final Bson collector) {
        return new SearchConstructibleBson(notNull("collector", collector));
    }
}

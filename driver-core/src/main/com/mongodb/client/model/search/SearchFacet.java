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
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.ToBsonField;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.internal.NumberHelper;

import java.time.Instant;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;
import static org.bson.assertions.Assertions.notNull;

/**
 * A facet definition for {@link FacetSearchCollector}.
 *
 * @mongodb.atlas.manual atlas-search/facet/#facet-definition Facet definition
 * @since 4.6
 */
@Beta
@Evolving
public interface SearchFacet extends ToBsonField {
    /**
     * Returns a {@link SearchFacet} that allows narrowing down search results based on the most frequent
     * values in the BSON {@link BsonType#STRING String} field specified by the {@code path}.
     *
     * @param name The facet name.
     * @param path The path to facet on.
     * @return The requested {@link SearchFacet}.
     * @mongodb.atlas.manual atlas-search/facet/#string-facets String facet definition
     */
    static StringSearchFacet stringFacet(final String name, final FieldSearchPath path) {
        return new ConstructibleBsonFieldToManifoldAdapter(notNull("name", name),
                new BsonDocument("type", new BsonString("string"))
                        .append("path", notNull("path", path).toBsonValue()));
    }

    /**
     * Returns a {@link SearchFacet} that allows determining the frequency of
     * BSON {@link BsonType#INT32 32-bit integer} / {@link BsonType#INT64 64-bit integer} / {@link BsonType#DOUBLE Double} values
     * in the search results by breaking the results into separate ranges.
     *
     * @param name The facet name.
     * @param path The path to facet on.
     * @param boundaries Bucket boundaries in ascending order. Must contain at least two boundaries.
     * The supported {@link Number} types are the same as those supported by {@code org.bson.BasicBSONEncoder.putNumber}.
     * @return The requested {@link SearchFacet}.
     * @mongodb.atlas.manual atlas-search/facet/#numeric-facets Numeric facet definition
     */
    static NumericSearchFacet numberFacet(final String name, final FieldSearchPath path, final Iterable<Number> boundaries) {
        final BsonArray boundariesArray = stream(notNull("boundaries", boundaries).spliterator(), false)
                .map(NumberHelper::toBsonNumber).collect(toCollection(BsonArray::new));
        isTrueArgument("boundaries must contain at least 2 elements", boundariesArray.size() >= 2);
        return new ConstructibleBsonFieldToManifoldAdapter(notNull("name", name),
                new BsonDocument("type", new BsonString("number"))
                        .append("path", notNull("path", path).toBsonValue())
                        .append("boundaries", notNull("boundaries", boundariesArray)));
    }

    /**
     * Returns a {@link SearchFacet} that allows determining the frequency of BSON {@link BsonType#DATE_TIME Date} values
     * in the search results by breaking the results into separate ranges.
     *
     * @param name The facet name.
     * @param path The path to facet on.
     * @param boundaries Bucket boundaries in ascending order. Must contain at least two boundaries.
     * @return The requested {@link SearchFacet}.
     * @mongodb.atlas.manual atlas-search/facet/#date-facets Date facet definition
     */
    static DateSearchFacet dateFacet(final String name, final FieldSearchPath path, final Iterable<Instant> boundaries) {
        final BsonArray boundariesArray = stream(notNull("boundaries", boundaries).spliterator(), false)
                .map(Instant::toEpochMilli).map(BsonDateTime::new).collect(toCollection(BsonArray::new));
        isTrueArgument("boundaries must contain at least 2 elements", boundariesArray.size() >= 2);
        return new ConstructibleBsonFieldToManifoldAdapter(notNull("name", name),
                new BsonDocument("type", new BsonString("date"))
                        .append("path", notNull("path", path).toBsonValue())
                        .append("boundaries", notNull("boundaries", boundariesArray)));
    }

    /**
     * Creates a {@link SearchFacet} from a {@link BsonField} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchFacet}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchFacet facet1 = SearchFacet.stringFacet("facetName",
     *          SearchPath.fieldPath("fieldName"));
     *  SearchFacet facet2 = SearchFacet.of(new BsonField("facetName", new BsonDocument("type", new BsonString("string"))
     *          .append("path", SearchPath.fieldPath("fieldName").toBsonValue())));
     * }</pre>
     *
     * @param facet A {@link BsonField} representing the required {@link SearchFacet}.
     * @return The requested {@link SearchFacet}.
     */
    static SearchFacet of(final BsonField facet) {
        return new BsonFieldToManifoldAdapter(notNull("facet", facet));
    }
}

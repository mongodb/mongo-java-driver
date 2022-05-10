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
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;
import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

/**
 * A facet definition for {@link FacetSearchCollector}.
 *
 * @mongodb.atlas.manual atlas-search/facet/#facet-definition Facet definition
 * @since 4.7
 */
@Beta
@Evolving
public interface SearchFacet extends Bson {
    /**
     * Returns a {@link SearchFacet} that allows narrowing down search results based on the most frequent
     * BSON {@link BsonType#STRING String} values of the specified field.
     *
     * @param name The facet name.
     * @param path The field to facet on.
     * @return The requested {@link SearchFacet}.
     * @mongodb.atlas.manual atlas-search/facet/#string-facets String facet definition
     */
    static StringSearchFacet stringFacet(final String name, final FieldSearchPath path) {
        return new SearchConstructibleBsonElement(notNull("name", name),
                new Document("type", "string")
                        .append("path", notNull("path", path).toValue()));
    }

    /**
     * Returns a {@link SearchFacet} that allows determining the frequency of
     * BSON {@link BsonType#INT32 32-bit integer} / {@link BsonType#INT64 64-bit integer} / {@link BsonType#DOUBLE Double} values
     * in the search results by breaking the results into separate ranges.
     *
     * @param name The facet name.
     * @param path The path to facet on.
     * @param boundaries Bucket boundaries in ascending order. Must contain at least two boundaries.
     * @return The requested {@link SearchFacet}.
     * @mongodb.atlas.manual atlas-search/facet/#numeric-facets Numeric facet definition
     */
    static NumberSearchFacet numberFacet(final String name, final FieldSearchPath path, final Iterable<? extends Number> boundaries) {
        isTrueArgument("boundaries must contain at least 2 elements", sizeAtLeast(boundaries, 2));
        return new SearchConstructibleBsonElement(notNull("name", name),
                new Document("type", "number")
                        .append("path", notNull("path", path).toValue())
                        .append("boundaries", notNull("boundaries", boundaries)));
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
        isTrueArgument("boundaries must contain at least 2 elements", sizeAtLeast(boundaries, 2));
        return new SearchConstructibleBsonElement(notNull("name", name),
                new Document("type", "date")
                        .append("path", notNull("path", path).toValue())
                        .append("boundaries", notNull("boundaries", boundaries)));
    }

    /**
     * Creates a {@link SearchFacet} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchFacet}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchFacet facet1 = SearchFacet.stringFacet("facetName",
     *          SearchPath.fieldPath("fieldName"));
     *  SearchFacet facet2 = SearchFacet.of(new Document("facetName", new Document("type", "string")
     *          .append("path", SearchPath.fieldPath("fieldName").toValue())));
     * }</pre>
     *
     * @param facet A {@link Bson} representing the required {@link SearchFacet}.
     * @return The requested {@link SearchFacet}.
     */
    static SearchFacet of(final Bson facet) {
        return new SearchConstructibleBson(notNull("facet", facet));
    }

    /**
     * Combines {@link SearchFacet}s into a {@link Bson}.
     * <p>
     * This method may be useful when using {@link SearchCollector#of(Bson)}.</p>
     *
     * @param facets The non-empty facet definitions to combine.
     * @return A {@link Bson} representing combined {@code facets}.
     */
    static Bson combineToBson(final Iterable<? extends SearchFacet> facets) {
        notNull("facets", facets);
        isTrueArgument("facets must not be empty", sizeAtLeast(facets, 1));
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                Set<String> names = new HashSet<>();
                BsonDocument result = new BsonDocument();
                for (final SearchFacet facet : facets) {
                    BsonDocument doc = facet.toBsonDocument(documentClass, codecRegistry);
                    assertTrue(doc.size() == 1);
                    Map.Entry<String, BsonValue> entry = doc.entrySet().iterator().next();
                    String name = entry.getKey();
                    isTrue(format("facet names must be unique. '%s' is used at least twice in %s", names, facets), names.add(name));
                    result.append(entry.getKey(), entry.getValue());
                }
                return result;
            }

            @Override
            public String toString() {
                return facets.toString();
            }
        };
    }
}

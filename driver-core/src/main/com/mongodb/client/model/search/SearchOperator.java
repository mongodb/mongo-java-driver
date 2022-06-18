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
import com.mongodb.client.model.geojson.Point;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.Iterables.concat;
import static com.mongodb.internal.client.model.Util.combineToBsonValue;
import static java.util.Collections.singleton;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The core part of the {@link Aggregates#search(SearchOperator, SearchOptions) $search} pipeline stage of an aggregation pipeline.
 *
 * @mongodb.atlas.manual atlas-search/operators-and-collectors/#operators Search operators
 * @since 4.7
 */
@Evolving
@Beta(Beta.Reason.CLIENT)
public interface SearchOperator extends Bson {
    /**
     * Creates a new {@link SearchOperator} with the scoring modifier specified.
     *
     * @param modifier The scoring modifier.
     * @return A new {@link SearchOperator}.
     */
    SearchOperator score(SearchScore modifier);

    /**
     * Returns a base for a {@link SearchOperator} that may combine multiple {@link SearchOperator}s.
     * Combining {@link SearchOperator}s affects calculation of the relevance score.
     *
     * @return A base for a {@link CompoundSearchOperator}.
     * @mongodb.atlas.manual atlas-search/compound/ compound operator
     */
    static CompoundSearchOperatorBase compound() {
        return new SearchConstructibleBsonElement("compound");
    }

    /**
     * Returns a {@link SearchOperator} that tests if the {@code path} exists in a document.
     *
     * @param path The path to test.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/exists/ exists operator
     */
    static ExistsSearchOperator exists(final FieldSearchPath path) {
        return new SearchConstructibleBsonElement("exists", new Document("path", notNull("path", path).toValue()));
    }

    /**
     * Returns a {@link SearchOperator} that performs a full-text search.
     *
     * @param path The field to be searched.
     * @param query The string to search for.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/text/ text operator
     */
    static TextSearchOperator text(final SearchPath path, final String query) {
        return text(singleton(notNull("path", path)), singleton(notNull("query", query)));
    }

    /**
     * Returns a {@link SearchOperator} that performs a full-text search.
     *
     * @param paths The non-empty fields to be searched.
     * @param queries The non-empty strings to search for.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/text/ text operator
     */
    static TextSearchOperator text(final Iterable<? extends SearchPath> paths, final Iterable<String> queries) {
        Iterator<String> queryIterator = notNull("queries", queries).iterator();
        isTrueArgument("queries must not be empty", queryIterator.hasNext());
        String firstQuery = queryIterator.next();
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        isTrueArgument("paths must not be empty", pathIterator.hasNext());
        return new SearchConstructibleBsonElement("text", new Document("query", queryIterator.hasNext() ? queries : firstQuery)
                .append("path", combineToBsonValue(pathIterator, false)));
    }

    /**
     * Returns a {@link SearchOperator} that may be used to implement search-as-you-type functionality.
     *
     * @param path The field to be searched.
     * @param query The string to search for.
     * @param queries More strings to search for.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/autocomplete/ autocomplete operator
     */
    static AutocompleteSearchOperator autocomplete(final FieldSearchPath path, final String query, final String... queries) {
        return autocomplete(path, concat(notNull("query", query), queries));
    }

    /**
     * Returns a {@link SearchOperator} that may be used to implement search-as-you-type functionality.
     *
     * @param path The field to be searched.
     * @param queries The non-empty strings to search for.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/autocomplete/ autocomplete operator
     */
    static AutocompleteSearchOperator autocomplete(final FieldSearchPath path, final Iterable<String> queries) {
        Iterator<String> queryIterator = notNull("queries", queries).iterator();
        isTrueArgument("queries must not be empty", queryIterator.hasNext());
        String firstQuery = queryIterator.next();
        return new SearchConstructibleBsonElement("autocomplete", new Document("query", queryIterator.hasNext() ? queries : firstQuery)
                .append("path", notNull("path", path).toValue()));
    }

    /**
     * Returns a base for a {@link SearchOperator} that tests if the
     * BSON {@link BsonType#INT32 32-bit integer} / {@link BsonType#INT64 64-bit integer} / {@link BsonType#DOUBLE Double} values
     * of the specified fields are within an interval.
     *
     * @param path The field to be searched.
     * @param paths More fields to be searched.
     * @return A base for a {@link NumberRangeSearchOperator}.
     * @mongodb.atlas.manual atlas-search/range/ range operator
     */
    static NumberRangeSearchOperatorBase numberRange(final FieldSearchPath path, final FieldSearchPath... paths) {
        return numberRange(concat(notNull("path", path), paths));
    }

    /**
     * Returns a base for a {@link SearchOperator} that tests if the
     * BSON {@link BsonType#INT32 32-bit integer} / {@link BsonType#INT64 64-bit integer} / {@link BsonType#DOUBLE Double} values
     * of the specified fields are within an interval.
     *
     * @param paths The non-empty fields to be searched.
     * @return A base for a {@link NumberRangeSearchOperator}.
     * @mongodb.atlas.manual atlas-search/range/ range operator
     */
    static NumberRangeSearchOperatorBase numberRange(final Iterable<? extends FieldSearchPath> paths) {
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        isTrueArgument("paths must not be empty", pathIterator.hasNext());
        return new NumberRangeConstructibleBsonElement("range", new Document("path", combineToBsonValue(pathIterator, true)));
    }

    /**
     * Returns a base for a {@link SearchOperator} that tests if the
     * BSON {@link BsonType#DATE_TIME Date} values of the specified fields are within an interval.
     *
     * @param path The field to be searched.
     * @param paths More fields to be searched.
     * @return A base for a {@link DateRangeSearchOperator}.
     * @mongodb.atlas.manual atlas-search/range/ range operator
     */
    static DateRangeSearchOperatorBase dateRange(final FieldSearchPath path, final FieldSearchPath... paths) {
        return dateRange(concat(notNull("path", path), paths));
    }

    /**
     * Returns a base for a {@link SearchOperator} that tests if the
     * BSON {@link BsonType#DATE_TIME Date} values of the specified fields are within an interval.
     *
     * @param paths The non-empty fields to be searched.
     * @return A base for a {@link DateRangeSearchOperator}.
     * @mongodb.atlas.manual atlas-search/range/ range operator
     */
    static DateRangeSearchOperatorBase dateRange(final Iterable<? extends FieldSearchPath> paths) {
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        isTrueArgument("paths must not be empty", pathIterator.hasNext());
        return new DateRangeConstructibleBsonElement("range", new Document("path", combineToBsonValue(pathIterator, true)));
    }

    /**
     * Returns a {@link SearchOperator} that allows finding results that are near the specified {@code origin}.
     *
     * @param origin The origin from which the proximity of the results is measured.
     * The relevance score is 1 if the values of the fields are {@code origin}.
     * @param pivot The positive distance from the {@code origin} at which the relevance score drops in half.
     * @param path The field to be searched.
     * @param paths More fields to be searched.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/near/ near operator
     */
    static NumberNearSearchOperator near(final Number origin, final Number pivot, final FieldSearchPath path, final FieldSearchPath... paths) {
        return near(origin, pivot, concat(notNull("path", path), paths));
    }

    /**
     * Returns a {@link SearchOperator} that allows finding results that are near the specified {@code origin}.
     *
     * @param origin The origin from which the proximity of the results is measured.
     * The relevance score is 1 if the values of the fields are {@code origin}.
     * @param pivot The positive distance from the {@code origin} at which the relevance score drops in half.
     * @param paths The non-empty fields to be searched.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/near/ near operator
     */
    static NumberNearSearchOperator near(final Number origin, final Number pivot, final Iterable<? extends FieldSearchPath> paths) {
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        isTrueArgument("paths must not be empty", pathIterator.hasNext());
        return new SearchConstructibleBsonElement("near", new Document("origin", notNull("origin", origin))
                .append("path", combineToBsonValue(pathIterator, true))
                .append("pivot", notNull("pivot", pivot)));
    }

    /**
     * Returns a {@link SearchOperator} that allows finding results that are near the specified {@code origin}.
     *
     * @param origin The origin from which the proximity of the results is measured.
     * The relevance score is 1 if the values of the fields are {@code origin}.
     * @param pivot The positive distance from the {@code origin} at which the relevance score drops in half.
     * Data is extracted via {@link Duration#toMillis()}.
     * @param path The field to be searched.
     * @param paths More fields to be searched.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/near/ near operator
     * @see org.bson.codecs.jsr310.InstantCodec
     */
    static DateNearSearchOperator near(final Instant origin, final Duration pivot, final FieldSearchPath path, final FieldSearchPath... paths) {
        return near(origin, pivot, concat(notNull("path", path), paths));
    }

    /**
     * Returns a {@link SearchOperator} that allows finding results that are near the specified {@code origin}.
     *
     * @param origin The origin from which the proximity of the results is measured.
     * The relevance score is 1 if the values of the fields are {@code origin}.
     * @param pivot The positive distance from the {@code origin} at which the relevance score drops in half.
     * Data is extracted via {@link Duration#toMillis()}.
     * @param paths The non-empty fields to be searched.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/near/ near operator
     * @see org.bson.codecs.jsr310.InstantCodec
     */
    static DateNearSearchOperator near(final Instant origin, final Duration pivot, final Iterable<? extends FieldSearchPath> paths) {
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        isTrueArgument("paths must not be empty", pathIterator.hasNext());
        notNull("pivot", pivot);
        isTrueArgument("pivot must be positive", !pivot.isZero());
        isTrueArgument("pivot must be positive", !pivot.isNegative());
        return new SearchConstructibleBsonElement("near", new Document("origin", notNull("origin", origin))
                .append("path", combineToBsonValue(pathIterator, true))
                .append("pivot", pivot.toMillis()));
    }

    /**
     * Returns a {@link SearchOperator} that allows finding results that are near the specified {@code origin}.
     *
     * @param origin The origin from which the proximity of the results is measured.
     * The relevance score is 1 if the values of the fields are {@code origin}.
     * @param pivot The positive distance in meters from the {@code origin} at which the relevance score drops in half.
     * @param path The field to be searched.
     * @param paths More fields to be searched.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/near/ near operator
     */
    static GeoNearSearchOperator near(final Point origin, final Number pivot, final FieldSearchPath path, final FieldSearchPath... paths) {
        return near(origin, pivot, concat(notNull("path", path), paths));
    }

    /**
     * Returns a {@link SearchOperator} that allows finding results that are near the specified {@code origin}.
     *
     * @param origin The origin from which the proximity of the results is measured.
     * The relevance score is 1 if the values of the fields are {@code origin}.
     * @param pivot The positive distance in meters from the {@code origin} at which the relevance score drops in half.
     * @param paths The non-empty fields to be searched.
     * @return The requested {@link SearchOperator}.
     * @mongodb.atlas.manual atlas-search/near/ near operator
     */
    static GeoNearSearchOperator near(final Point origin, final Number pivot, final Iterable<? extends FieldSearchPath> paths) {
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        isTrueArgument("paths must not be empty", pathIterator.hasNext());
        return new SearchConstructibleBsonElement("near", new Document("origin", notNull("origin", origin))
                .append("path", combineToBsonValue(pathIterator, true))
                .append("pivot", notNull("pivot", pivot)));
    }

    /**
     * Creates a {@link SearchOperator} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchOperator}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchOperator operator1 = SearchOperator.exists(
     *          SearchPath.fieldPath("fieldName"));
     *  SearchOperator operator2 = SearchOperator.of(new Document("exists",
     *          new Document("path", SearchPath.fieldPath("fieldName").toValue())));
     * }</pre>
     *
     * @param operator A {@link Bson} representing the required {@link SearchOperator}.
     * @return The requested {@link SearchOperator}.
     */
    static SearchOperator of(final Bson operator) {
        return new SearchConstructibleBsonElement(notNull("operator", operator));
    }
}

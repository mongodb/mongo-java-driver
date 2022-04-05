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

import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.Month;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isAtlasSearchTest;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceWith;
import static com.mongodb.client.model.Aggregates.search;
import static com.mongodb.client.model.Projections.computedSearchMeta;
import static com.mongodb.client.model.Projections.metaSearchHighlights;
import static com.mongodb.client.model.Projections.metaSearchScore;
import static com.mongodb.client.model.search.SearchCollector.facet;
import static com.mongodb.client.model.search.SearchCount.lowerBound;
import static com.mongodb.client.model.search.SearchFacet.dateFacet;
import static com.mongodb.client.model.search.SearchFacet.numberFacet;
import static com.mongodb.client.model.search.SearchFacet.stringFacet;
import static com.mongodb.client.model.search.SearchHighlight.paths;
import static com.mongodb.client.model.search.SearchOperator.exists;
import static com.mongodb.client.model.search.SearchOptions.defaultSearchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * These tests require the following Atlas Search index for the {@code sample_mflix.movies} namespace:
 * <pre>{@code
 *  {
 *    "mappings": {
 *      "dynamic": true,
 *      "fields": {
 *        "fullplot": {
 *          "type": "stringFacet"
 *        },
 *        "released": {
 *          "type": "dateFacet"
 *        },
 *        "title": {
 *          "multi": {
 *            "keyword": {
 *              "analyzer": "lucene.keyword",
 *              "type": "string"
 *            }
 *          },
 *          "type": "string"
 *        },
 *        "tomatoes.viewer.meter": {
 *          "type": "numberFacet"
 *        }
 *      }
 *    },
 *    "storedSource": {
 *      "include": [
 *        "plot"
 *      ]
 *    }
 *  }
 * }</pre>
 */
final class AggregatesSearchIntegrationTest {
    private CollectionHelper<BsonDocument> collectionHelper;

    @BeforeEach
    void beforeEach() {
        assumeTrue(isAtlasSearchTest());
        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), new MongoNamespace("sample_mflix", "movies"));
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("args")
    void test(
            final Bson stageUnderTest,
            @Nullable final Iterable<Bson> postStages,
            final Consumer<List<BsonDocument>> resultAsserter) {
        final List<Bson> pipeline = new ArrayList<>();
        pipeline.add(stageUnderTest);
        if (postStages != null) {
            postStages.forEach(pipeline::add);
        }
        resultAsserter.accept(collectionHelper.aggregate(pipeline));
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                arguments(
                        search(
                                exists(fieldPath("tomatoes.dvd")),
                                null
                        ),
                        asList(limit(1), project(metaSearchScore("score"))),
                        Asserters.score(BigDecimal.ONE)
                ),
                arguments(
                        search(
                                exists(fieldPath("tomatoes")),
                                defaultSearchOptions()
                                        .index("default")
                                        .count(lowerBound().threshold(1_000))
                        ),
                        asList(limit(1), project(computedSearchMeta("meta"))),
                        Asserters.countLowerBound(1_000)
                ),
                arguments(
                        search(
                                exists(fieldPath("plot")),
                                defaultSearchOptions()
                                        .returnStoredSource(true)
                        ),
                        singleton(limit(1)),
                        Asserters.firstResult(doc -> {
                            // assert that the fields specified in `storedSource` and "id" were returned
                            assertNotNull(doc.get("_id"));
                            assertFalse(doc.get("plot").asString().getValue().isEmpty());
                            assertEquals(2, doc.size());
                        })
                ),
                arguments(
                        search(
                                facet(
                                        exists(fieldPath("tomatoes")),
                                        asList(
                                                stringFacet(
                                                        "fullplotFacet",
                                                        fieldPath("fullplot"))
                                                        .numBuckets(1),
                                                numberFacet(
                                                        "tomatoesMeterFacet",
                                                        fieldPath("tomatoes.viewer.meter"),
                                                        asList(10f, 20d, 90, Long.MAX_VALUE / 2, Long.MAX_VALUE))
                                                        .defaultBucket("defaultBucket"),
                                                dateFacet(
                                                        "releasedFacet",
                                                        fieldPath("released"),
                                                        asList(
                                                                Instant.EPOCH,
                                                                Instant.from(Year.of(1985)
                                                                        .atMonth(Month.JANUARY).atDay(1).atStartOfDay().atOffset(UTC)),
                                                                Instant.now()))
                                        )),
                                defaultSearchOptions()
                        ),
                        asList(limit(1), project(computedSearchMeta("meta")), replaceWith("$meta")),
                        Asserters.firstResult(doc -> assertEquals(5, doc.getDocument("facet")
                                .getDocument("tomatoesMeterFacet").getArray("buckets").size()))
                ),
                arguments(
                        search(
                                exists(fieldPath("tomatoes")),
                                defaultSearchOptions()
                                        .highlight(paths(
                                                singleton(wildcardPath("pl*t")))
                                                .maxCharsToExamine(100))
                        ),
                        asList(limit(1), project(metaSearchHighlights("highlights"))),
                        Asserters.nonEmpty()
                )
        );
    }

    private static final class Asserters {
        private static Consumer<List<BsonDocument>> nonEmpty() {
            return results -> assertFalse(results.isEmpty());
        }

        /**
         * Checks the value of the {@code "score"} field.
         */
        private static Consumer<List<BsonDocument>> score(final BigDecimal expectedScore) {
            return results -> {
                assertFalse(results.isEmpty());
                for (BsonDocument result : results) {
                    assertEquals(expectedScore, result.getNumber("score").decimal128Value().bigDecimalValue());
                }
            };
        }

        /**
         * Checks the value of the {@code "meta.count.lowerBound"} field.
         */
        private static Consumer<List<BsonDocument>> countLowerBound(final int expectedAtLeast) {
            return firstResult(doc -> assertTrue(doc.getDocument("meta")
                    .getDocument("count")
                    .getNumber("lowerBound")
                    .intValue() >= expectedAtLeast));
        }

        private static Consumer<List<BsonDocument>> firstResult(final Consumer<BsonDocument> asserter) {
            return results -> {
                assertFalse(results.isEmpty());
                asserter.accept(results.get(0));
            };
        }
    }
}

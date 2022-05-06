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
import com.mongodb.assertions.Assertions;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.Month;
import java.time.Year;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isAtlasSearchTest;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceWith;
import static com.mongodb.client.model.Projections.computedSearchMeta;
import static com.mongodb.client.model.Projections.metaSearchHighlights;
import static com.mongodb.client.model.Projections.metaSearchScore;
import static com.mongodb.client.model.search.SearchFuzzy.defaultSearchFuzzy;
import static com.mongodb.client.model.search.SearchCollector.facet;
import static com.mongodb.client.model.search.SearchCount.lowerBound;
import static com.mongodb.client.model.search.SearchCount.total;
import static com.mongodb.client.model.search.SearchFacet.dateFacet;
import static com.mongodb.client.model.search.SearchFacet.numberFacet;
import static com.mongodb.client.model.search.SearchFacet.stringFacet;
import static com.mongodb.client.model.search.SearchHighlight.paths;
import static com.mongodb.client.model.search.SearchOperator.compound;
import static com.mongodb.client.model.search.SearchOperator.exists;
import static com.mongodb.client.model.search.SearchOperator.text;
import static com.mongodb.client.model.search.SearchOptions.defaultSearchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static com.mongodb.client.model.search.SearchScore.boost;
import static com.mongodb.client.model.search.SearchScore.constant;
import static com.mongodb.client.model.search.SearchScore.function;
import static com.mongodb.client.model.search.SearchScoreExpression.addExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.constantExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.gaussExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.log1pExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.logExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.multiplyExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.pathExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.relevanceExpression;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * These tests require the <a href="https://www.mongodb.com/docs/atlas/sample-data/">sample data</a>
 * and the following Atlas Search index for the {@code sample_mflix.movies} namespace:
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

    /**
     * @param stageUnderTestCreator A {@link CustomizableSearchStageCreator} that is used to create both
     * {@code $search} and {@code $searchMeta} stages. Any combination of an {@link SearchOperator}/{@link SearchCollector} and
     * {@link SearchOptions} that is valid for the {@code $search} stage is also valid for the {@code $searchMeta} stage.
     * This is why we use the same creator for both.
     * @param accessories A list of {@link Accessory} objects that specify additional pipeline stages and an asserter.
     * <ul>
     *  <li>The item with index 0 is used with {@code $search};</li>
     *  <li>the idem with index 1 is used with {@code $searchMeta}.</li>
     * </ul>
     */
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("args")
    void test(
            @SuppressWarnings("unused") final String testDescription,
            final CustomizableSearchStageCreator stageUnderTestCreator,
            final List<Accessory> accessories) {
        List<BiFunction<Bson, SearchOptions, Bson>> stageUnderTestCustomizers = asList(
                (bsonOperatorOrCollector, options) -> {
                    if (bsonOperatorOrCollector instanceof SearchOperator) {
                        return Aggregates.search((SearchOperator) bsonOperatorOrCollector, options);
                    } else if (bsonOperatorOrCollector instanceof SearchCollector) {
                        return Aggregates.search((SearchCollector) bsonOperatorOrCollector, options);
                    } else {
                        throw Assertions.fail();
                    }
                },
                (bsonOperatorOrCollector, options) -> {
                    if (bsonOperatorOrCollector instanceof SearchOperator) {
                        return Aggregates.searchMeta((SearchOperator) bsonOperatorOrCollector, options);
                    } else if (bsonOperatorOrCollector instanceof SearchCollector) {
                        return Aggregates.searchMeta((SearchCollector) bsonOperatorOrCollector, options);
                    } else {
                        throw Assertions.fail();
                    }
                }
        );
        Assertions.assertTrue(stageUnderTestCustomizers.size() == accessories.size());
        for (int i = 0; i < stageUnderTestCustomizers.size(); i++) {
            Bson stageUnderTest = stageUnderTestCreator.apply(stageUnderTestCustomizers.get(i));
            Accessory accessory = accessories.get(i);
            final List<Bson> pipeline = new ArrayList<>();
            pipeline.add(stageUnderTest);
            pipeline.addAll(accessory.postStages);
            Supplier<String> msgSupplier = () -> "For reference, the pipeline (" + pipeline.size() + " elements) used in the test is\n[\n"
                    + pipeline.stream()
                    .map(Bson::toBsonDocument)
                    .map(doc -> doc.toJson(JsonWriterSettings.builder().indent(true).build()))
                    .collect(Collectors.joining(",\n"))
                    + "\n]\n";
            List<BsonDocument> results;
            try {
                results = collectionHelper.aggregate(pipeline);
            } catch (RuntimeException e) {
                throw new RuntimeException(msgSupplier.get(), e);
            }
            accessory.resultAsserter.accept(results, msgSupplier);
        }
    }

    /**
     * @see #test(String, CustomizableSearchStageCreator, List)
     */
    private static Stream<Arguments> args() {
        return Stream.of(
                arguments(
                        "default options",
                        stageCreator(
                                exists(fieldPath("tomatoes.dvd")),
                                null
                        ),
                        asList(
                                new Accessory(
                                        asList(limit(1), project(metaSearchScore("score"))),
                                        Asserters.score(1)
                                ),
                                new Accessory(
                                        emptyList(),
                                        // specifying a bare operator works as if `SearchCount.lowerBound` were specified
                                        Asserters.countLowerBound(1_001)
                                )
                        )
                ),
                arguments(
                        "`index`, `count` options",
                        stageCreator(
                                // `multi` is used here only to verify that it is tolerated
                                exists(fieldPath("title").multi("keyword")),
                                defaultSearchOptions()
                                        .option("index", "default")
                                        .count(lowerBound().threshold(2_000))
                        ),
                        asList(
                                new Accessory(
                                        asList(limit(1), project(computedSearchMeta("meta"))),
                                        Asserters.countLowerBound("meta", 2_000)
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countLowerBound(2_000)
                                )
                        )
                ),
                arguments(
                        "`highlight` option",
                        stageCreator(
                                text(asList("factory", "century"), singleton(fieldPath("plot"))),
                                defaultSearchOptions()
                                        .highlight(paths(asList(
                                                fieldPath("title").multi("keyword"),
                                                wildcardPath("pl*t")))
                                                .maxCharsToExamine(100_000))
                        ),
                        asList(
                                new Accessory(
                                        asList(limit(1), project(metaSearchHighlights("highlights"))),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(1, doc.getArray("highlights").size(), msgSupplier))
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                )
                        )
                ),
                arguments(
                        "`returnStoredSource` option",
                        stageCreator(
                                exists(fieldPath("plot")),
                                defaultSearchOptions()
                                        .returnStoredSource(true)
                        ),
                        asList(
                                new Accessory(
                                        singleton(limit(1)),
                                        Asserters.firstResult((doc, msgSupplier) -> {
                                            // assert that the fields specified in `storedSource` and "id" were returned
                                            assertNotNull(doc.get("_id"), msgSupplier);
                                            assertFalse(doc.get("plot").asString().getValue().isEmpty(), msgSupplier);
                                            assertEquals(2, doc.size(), msgSupplier);
                                        })
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                )
                        )
                ),
                arguments(
                        "alternate analyzer (`multi` field path)",
                        stageCreator(
                                text(singleton("The Cheat"), singleton(fieldPath("title").multi("keyword"))),
                                defaultSearchOptions().count(total())
                        ),
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(
                                                "The Cheat", doc.getString("title").getValue(), msgSupplier))
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countTotal(1)
                                )
                        )
                ),
                arguments(
                        "facet collector",
                        stageCreator(
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
                                                                Instant.now())))),
                                defaultSearchOptions()
                        ),
                        asList(
                                new Accessory(
                                        asList(limit(1), project(computedSearchMeta("meta")), replaceWith("$meta")),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(5, doc.getDocument("facet")
                                                .getDocument("tomatoesMeterFacet").getArray("buckets").size(), msgSupplier))
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(5, doc.getDocument("facet")
                                                .getDocument("tomatoesMeterFacet").getArray("buckets").size(), msgSupplier))
                                )
                        )
                ),
                arguments(
                        "score modifier",
                        stageCreator(compound()
                                .should(asList(
                                        exists(fieldPath("fieldName1"))
                                                .score(boost(Float.MAX_VALUE / 2)),
                                        exists(fieldPath("fieldName2"))
                                                .score(boost(fieldPath("boostFieldName"))),
                                        exists(fieldPath("fieldName3"))
                                                .score(boost(fieldPath("boostFieldName"))
                                                        .undefined(-1)),
                                        exists(fieldPath("fieldName4"))
                                                .score(constant(1.2f)),
                                        exists(fieldPath("fieldName5"))
                                                .score(function(relevanceExpression())),
                                        exists(fieldPath("fieldName6"))
                                                .score(function(pathExpression(fieldPath("expressionFieldName")))),
                                        exists(fieldPath("fieldName7"))
                                                .score(function(pathExpression(fieldPath("expressionFieldName"))
                                                        .undefined(-1))),
                                        exists(fieldPath("fieldName8"))
                                                .score(function(constantExpression(-1.2f))),
                                        exists(fieldPath("fieldName9"))
                                                .score(function(
                                                        gaussExpression(-10, pathExpression(fieldPath("gaussianFeldName")), Double.MAX_VALUE / -2))),
                                        exists(fieldPath("fieldName10"))
                                                .score(function(
                                                        gaussExpression(
                                                                -10,
                                                                pathExpression(fieldPath("gaussianFeldName"))
                                                                        .undefined(0),
                                                                Double.MAX_VALUE / -2)
                                                        .offset(Double.MAX_VALUE / -2)
                                                        .decay(Double.MIN_VALUE))),
                                        exists(fieldPath("fieldName11"))
                                                .score(function(logExpression(constantExpression(3)))),
                                        exists(fieldPath("fieldName12"))
                                                .score(function(log1pExpression(constantExpression(-3)))),
                                        exists(fieldPath("fieldName13"))
                                                .score(function(addExpression(asList(
                                                        logExpression(multiplyExpression(asList(
                                                                constantExpression(2),
                                                                constantExpression(3),
                                                                relevanceExpression()))),
                                                        gaussExpression(0, pathExpression(fieldPath("gaussianFeldName")), 1)))))
                                )),
                                null
                        ),
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.empty()
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                )
                        )
                ),
                arguments(
                        "all operators in a `compound` operator",
                        stageCreator(compound()
                                .should(asList(
                                        exists(fieldPath("fieldName1")),
                                        text("term1", fieldPath("fieldName2")),
                                        text(asList("term2", "term3"), singleton(wildcardPath("wildc*rd")))
                                                .fuzzy(defaultSearchFuzzy()
                                                        .maxEdits(1)
                                                        .prefixLength(2)
                                                        .maxExpansions(3))))
                                .minimumShouldMatch(1)
                                .mustNot(singleton(
                                        compound().must(singleton(exists(fieldPath("fieldName3")))))),
                                null
                        ),
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.empty()
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countLowerBound(0)
                                )
                        )
                )
        );
    }

    private static final class Asserters {
        static Asserter empty() {
            return decorate((results, msgSupplier) -> assertTrue(results.isEmpty(), msgSupplier));
        }

        static Asserter nonEmpty() {
            return decorate((results, msgSupplier) -> assertFalse(results.isEmpty(), msgSupplier));
        }

        /**
         * Checks the value of the {@code "score"} field for each result document.
         */
        static Asserter score(final double expectedScore) {
            return decorate((results, msgSupplier) -> {
                assertFalse(results.isEmpty(), msgSupplier);
                for (BsonDocument result : results) {
                    assertEquals(expectedScore, result.getNumber("score").doubleValue(), 0.000_1, msgSupplier);
                }
            });
        }

        /**
         * Checks the value of the {@code "customMetaField.count.lowerBound"} field.
         */
        static Asserter countLowerBound(final String customMetaField, final int expectedAtLeast) {
            return firstResult((doc, msgSupplier) -> assertTrue(
                    doc.getDocument(customMetaField).getDocument("count").getNumber("lowerBound").intValue() >= expectedAtLeast, msgSupplier));
        }

        /**
         * Checks the value of the {@code "count.lowerBound"} field.
         */
        static Asserter countLowerBound(final int expectedAtLeast) {
            return firstResult((doc, msgSupplier) -> assertTrue(
                    doc.getDocument("count").getNumber("lowerBound").intValue() >= expectedAtLeast, msgSupplier));
        }

        /**
         * Checks the value of the {@code "count.total"} field.
         */
        static Asserter countTotal(final int expected) {
            return firstResult((doc, msgSupplier) -> assertEquals(
                    expected, doc.getDocument("count").getNumber("total").intValue(), msgSupplier));
        }

        static Asserter firstResult(final BiConsumer<BsonDocument, Supplier<String>> asserter) {
            return decorate((results, msgSupplier) -> {
                assertFalse(results.isEmpty(), msgSupplier);
                asserter.accept(results.get(0), msgSupplier);
            });
        }

        private static Asserter decorate(final Asserter asserter) {
            int maxRenderedResults = 20;
            return (results, msgSupplier) -> asserter.accept(
                    results,
                    () -> msgSupplier.get()
                            + "\ntop " + maxRenderedResults + " out of total " + results.size() + " results are\n["
                            + results.stream()
                            .map(doc -> doc.toJson(JsonWriterSettings.builder().indent(true).build()))
                            .limit(maxRenderedResults)
                            .collect(Collectors.joining(",\n"))
                            + "\n]\n"
            );
        }
    }

    private static CustomizableSearchStageCreator stageCreator(final Bson operatorOrCollector, @Nullable final SearchOptions options) {
        return customizer -> customizer.apply(operatorOrCollector, options);
    }

    @FunctionalInterface
    private interface CustomizableSearchStageCreator extends Function<BiFunction<Bson, SearchOptions, Bson>, Bson> {
    }

    @FunctionalInterface
    private interface Asserter extends BiConsumer<List<BsonDocument>, Supplier<String>> {
    }

    private static final class Accessory {
        private final Collection<Bson> postStages;
        private final BiConsumer<List<BsonDocument>, Supplier<String>> resultAsserter;

        Accessory(final Collection<Bson> postStages, final BiConsumer<List<BsonDocument>, Supplier<String>> resultAsserter) {
            this.postStages = postStages;
            this.resultAsserter = resultAsserter;
        }
    }
}

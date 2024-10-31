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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.SearchIndexType;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.operation.SearchIndexRequest;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.Vector;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isAtlasSearchTest;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.metaVectorSearchScore;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchOptions.exactVectorSearchOptions;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class AggregatesVectorSearchIntegrationTest {
    private static final String VECTOR_INDEX = "vector_search_index";
    private static final String VECTOR_FIELD_INT_8 = "int8Vector";
    private static final String VECTOR_FIELD_FLOAT_32 = "float32Vector";
    private static final String VECTOR_FIELD_LEGACY_DOUBLE_LIST = "legacyDoubleVector";
    private static final int LIMIT = 5;
    private static final String FIELD_YEAR = "year";
    private static CollectionHelper<Document> collectionHelper;
    private static final BsonDocument VECTOR_SEARCH_DEFINITION = BsonDocument.parse(
            "{"
                    + "  fields: ["
                    + "     {"
                    + "       path: '" + VECTOR_FIELD_INT_8 + "',"
                    + "       numDimensions: 5,"
                    + "       similarity: 'cosine',"
                    + "       type: 'vector',"
                    + "     },"
                    + "     {"
                    + "       path: '" + VECTOR_FIELD_FLOAT_32 + "',"
                    + "       numDimensions: 5,"
                    + "       similarity: 'cosine',"
                    + "       type: 'vector',"
                    + "     },"
                    + "     {"
                    + "       path: '" + VECTOR_FIELD_LEGACY_DOUBLE_LIST + "',"
                    + "       numDimensions: 5,"
                    + "       similarity: 'cosine',"
                    + "       type: 'vector',"
                    + "     },"
                    + "     {"
                    + "       path: '" + FIELD_YEAR + "',"
                    + "       type: 'filter',"
                    + "     },"
                    + "  ]"
                    + "}");

    @BeforeAll
    static void beforeAll() {
        assumeTrue(isAtlasSearchTest());
        assumeTrue(serverVersionAtLeast(6, 0));

        collectionHelper =
                new CollectionHelper<>(new DocumentCodec(), new MongoNamespace("javaVectorSearchTest", AggregatesVectorSearchIntegrationTest.class.getSimpleName()));
        collectionHelper.drop();
        collectionHelper.insertDocuments(
                new Document()
                        .append("_id", 0)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{0, 1, 2, 3, 4}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{0.0001, 1.12345, 2.23456, 3.34567, 4.45678})
                        .append(FIELD_YEAR, 2016),
                new Document()
                        .append("_id", 1)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{1, 2, 3, 4, 5}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{1.0001f, 2.12345f, 3.23456f, 4.34567f, 5.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{1.0001, 2.12345, 3.23456, 4.34567, 5.45678})
                        .append(FIELD_YEAR, 2017),
                new Document()
                        .append("_id", 2)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{2, 3, 4, 5, 6}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{2.0002f, 3.12345f, 4.23456f, 5.34567f, 6.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{2.0002, 3.12345, 4.23456, 5.34567, 6.45678})
                        .append(FIELD_YEAR, 2018),
                new Document()
                        .append("_id", 3)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{3, 4, 5, 6, 7}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{3.0003f, 4.12345f, 5.23456f, 6.34567f, 7.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{3.0003, 4.12345, 5.23456, 6.34567, 7.45678})
                        .append(FIELD_YEAR, 2019),
                new Document()
                        .append("_id", 4)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{4, 5, 6, 7, 8}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{4.0004f, 5.12345f, 6.23456f, 7.34567f, 8.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{4.0004, 5.12345, 6.23456, 7.34567, 8.45678})
                        .append(FIELD_YEAR, 2020),
                new Document()
                        .append("_id", 5)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{5, 6, 7, 8, 9}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{5.0005f, 6.12345f, 7.23456f, 8.34567f, 9.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{5.0005, 6.12345, 7.23456, 8.34567, 9.45678})
                        .append(FIELD_YEAR, 2021),
                new Document()
                        .append("_id", 6)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{6, 7, 8, 9, 10}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{6.0006f, 7.12345f, 8.23456f, 9.34567f, 10.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{6.0006, 7.12345, 8.23456, 9.34567, 10.45678})
                        .append(FIELD_YEAR, 2022),
                new Document()
                        .append("_id", 7)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{7, 8, 9, 10, 11}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{7.0007f, 8.12345f, 9.23456f, 10.34567f, 11.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{7.0007, 8.12345, 9.23456, 10.34567, 11.45678})
                        .append(FIELD_YEAR, 2023),
                new Document()
                        .append("_id", 8)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{8, 9, 10, 11, 12}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{8.0008f, 9.12345f, 10.23456f, 11.34567f, 12.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{8.0008, 9.12345, 10.23456, 11.34567, 12.45678})
                        .append(FIELD_YEAR, 2024),
                new Document()
                        .append("_id", 9)
                        .append(VECTOR_FIELD_INT_8, Vector.int8Vector(new byte[]{9, 10, 11, 12, 13}))
                        .append(VECTOR_FIELD_FLOAT_32, Vector.floatVector(new float[]{9.0009f, 10.12345f, 11.23456f, 12.34567f, 13.45678f}))
                        .append(VECTOR_FIELD_LEGACY_DOUBLE_LIST, new double[]{9.0009, 10.12345, 11.23456, 12.34567, 13.45678})
                        .append(FIELD_YEAR, 2025)
        );

        collectionHelper.createSearchIndex(
                new SearchIndexRequest(VECTOR_SEARCH_DEFINITION, VECTOR_INDEX,
                        SearchIndexType.vectorSearch()));
        awaitIndexCreation();
    }

    @AfterAll
    static void afterAll() {
        if (collectionHelper != null) {
            collectionHelper.drop();
        }
    }

    private static Stream<Arguments> provideSupportedVectors() {
        return Stream.of(
                arguments(Vector.int8Vector(new byte[]{0, 1, 2, 3, 4}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_INT_8).multi("ignored"),
                        approximateVectorSearchOptions(LIMIT * 2)),
                arguments(Vector.int8Vector(new byte[]{0, 1, 2, 3, 4}),
                        fieldPath(VECTOR_FIELD_INT_8),
                        approximateVectorSearchOptions(LIMIT * 2)),

                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_FLOAT_32).multi("ignored"),
                        approximateVectorSearchOptions(LIMIT * 2)),
                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_FLOAT_32),
                        approximateVectorSearchOptions(LIMIT * 2)),

                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_FLOAT_32).multi("ignored"),
                        exactVectorSearchOptions()),
                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_FLOAT_32),
                        exactVectorSearchOptions()),

                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST).multi("ignored"),
                        exactVectorSearchOptions()),
                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST),
                        exactVectorSearchOptions()),

                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST).multi("ignored"),
                        approximateVectorSearchOptions(LIMIT * 2)),
                arguments(Vector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST),
                        approximateVectorSearchOptions(LIMIT * 2))
        );
    }

    @ParameterizedTest
    @MethodSource("provideSupportedVectors")
    void shouldSearchByVectorWithSearchScore(final Vector vector,
                                                      final FieldSearchPath fieldSearchPath,
                                                      final VectorSearchOptions vectorSearchOptions) {
        //given
        List<Bson> pipeline = asList(
                Aggregates.vectorSearch(
                        fieldSearchPath,
                        vector,
                        VECTOR_INDEX, LIMIT,
                        vectorSearchOptions),
                Aggregates.project(
                        fields(
                                metaVectorSearchScore("vectorSearchScore")
                        ))
        );

        //when
        List<Document> aggregate = collectionHelper.aggregate(pipeline);

        //then
        Assertions.assertEquals(LIMIT, aggregate.size());
        assertScoreIsDecreasing(aggregate);
        Document highestScoreDocument = aggregate.get(0);
        assertEquals(1, highestScoreDocument.getDouble("vectorSearchScore"));
    }

    @ParameterizedTest
    @MethodSource("provideSupportedVectors")
    void shouldSearchByVector(final Vector vector,
                                       final FieldSearchPath fieldSearchPath,
                                       final VectorSearchOptions vectorSearchOptions) {
        //given
        List<Bson> pipeline = asList(
                Aggregates.vectorSearch(
                        fieldSearchPath,
                        vector,
                        VECTOR_INDEX, LIMIT,
                        vectorSearchOptions)
        );

        //when
        List<Document> aggregate = collectionHelper.aggregate(pipeline);

        //then
        Assertions.assertEquals(LIMIT, aggregate.size());
        assertFalse(
                aggregate.stream()
                        .anyMatch(document -> document.containsKey("vectorSearchScore"))
        );
    }

    @ParameterizedTest
    @MethodSource("provideSupportedVectors")
    void shouldSearchByVectorWithFilter(final Vector vector,
                                 final FieldSearchPath fieldSearchPath,
                                 final VectorSearchOptions vectorSearchOptions) {
        Consumer<Bson> asserter = filter -> {
            List<Bson> pipeline = singletonList(
                    Aggregates.vectorSearch(
                            fieldSearchPath, vector, VECTOR_INDEX, 1,
                            vectorSearchOptions.filter(filter))
            );

            List<Document> aggregate = collectionHelper.aggregate(pipeline);
            Assertions.assertFalse(aggregate.isEmpty());
        };

        assertAll(
                () -> asserter.accept(lt("year", 2020)),
                () -> asserter.accept(lte("year", 2020)),
                () -> asserter.accept(eq("year", 2020)),
                () -> asserter.accept(gte("year", 2016)),
                () -> asserter.accept(gt("year", 2015)),
                () -> asserter.accept(ne("year", 2016)),
                () -> asserter.accept(in("year", 2000, 2024)),
                () -> asserter.accept(nin("year", 2000, 2024)),
                () -> asserter.accept(and(gte("year", 2015), lte("year", 2017))),
                () -> asserter.accept(or(eq("year", 2015), eq("year", 2017)))
        );
    }

    private static void assertScoreIsDecreasing(final List<Document> aggregate) {
        double previousScore = Integer.MAX_VALUE;
        for (Document document : aggregate) {
            Double vectorSearchScore = document.getDouble("vectorSearchScore");
            assertTrue(vectorSearchScore > 0, "Expected positive score");
            assertTrue(vectorSearchScore < previousScore, "Expected decreasing score");
            previousScore = vectorSearchScore;
        }
    }

    private static void awaitIndexCreation() {
        int attempts = 10;
        Optional<Document> searchIndex = Optional.empty();

        while (attempts-- > 0) {
            searchIndex = collectionHelper.listSearchIndex(VECTOR_INDEX);
            if (searchIndex.filter(document -> document.getBoolean("queryable"))
                    .isPresent()) {
                return;
            }

            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        String message = "Exceeded maximum attempts waiting for Search Index creation in Atlas cluster.";
        searchIndex.ifPresent(document -> Assertions.fail(message + " Index document: " + document.toJson()));
        Assertions.fail(message);
    }
}

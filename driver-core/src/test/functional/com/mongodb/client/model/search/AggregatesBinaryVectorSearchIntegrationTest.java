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
import com.mongodb.client.test.CollectionHelper;
import org.bson.BinaryVector;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
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


/**
 * This test runs on an atlas qa cluster in the `javaExtraTests.binaryVectorTests` namespace.
 * With readOnly user permissions.
 * <p>
 * With the following index:
 * <code>
 * {
 *     "name": "vector_search_index", "type": "vectorSearch",
 *     "definition": {"fields": [
 *         {"path": "int8Vector", "numDimensions": 5, "similarity": "cosine", "type": "vector"},
 *         {"path": "float32Vector", "numDimensions": 5, "similarity": "cosine", "type": "vector"},
 *         {"path": "legacyDoubleVector", "numDimensions": 5, "similarity": "cosine", "type": "vector"},
 *         {"path": "year", "type": "filter"}]}
 * }
 * </code>
 * <p>
 * And the following test data:
 * <code>
 * [{"_id":0, "int8Vector":{"$binary":{"base64":"AwAAAQIDBA==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwAXt9E4Ns2PPwgDD0B1H1ZA8Z2OQA==", "subType":"09"}},
 * "legacyDoubleVector":[0.0001,1.12345,2.23456,3.34567,4.45678], "year":2016},
 * {"_id":1, "int8Vector":{"$binary":{"base64":"AwABAgMEBQ==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwBHA4A/m+YHQAgDT0C7D4tA8Z2uQA==", "subType":"09"}},
 * "legacyDoubleVector":[1.0001,2.12345,3.23456,4.34567,5.45678], "year":2017},
 * {"_id":2, "int8Vector":{"$binary":{"base64":"AwACAwQFBg==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwBHAwBAm+ZHQISBh0C7D6tA8Z3OQA==", "subType":"09"}},
 * "legacyDoubleVector":[2.0002,3.12345,4.23456,5.34567,6.45678], "year":2018}},
 * {"_id":3, "int8Vector":{"$binary":{"base64":"AwADBAUGBw==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwDqBEBATfODQISBp0C7D8tA8Z3uQA==", "subType":"09"}},
 * "legacyDoubleVector":[3.0003,4.12345,5.23456,6.34567,7.45678], "year":2019}},
 * {"_id":4, "int8Vector":{"$binary":{"base64":"AwAEBQYHCA==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwBHA4BATfOjQISBx0C7D+tA+U4HQQ==", "subType":"09"}},
 * "legacyDoubleVector":[4.0004,5.12345,6.23456,7.34567,8.45678], "year":2020}},
 * {"_id":5, "int8Vector":{"$binary":{"base64":"AwAFBgcICQ==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwAZBKBATfPDQISB50DdhwVB+U4XQQ==", "subType":"09"}},
 * "legacyDoubleVector":[5.0005,6.12345,7.23456,8.34567,9.45678], "year":2021}},
 * {"_id":6, "int8Vector":{"$binary":{"base64":"AwAGBwgJCg==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwDqBMBATfPjQMLAA0HdhxVB+U4nQQ==", "subType":"09"}},
 * "legacyDoubleVector":[6.0006,7.12345,8.23456,9.34567,10.45678], "year":2022}},
 * {"_id":7, "int8Vector":{"$binary":{"base64":"AwAHCAkKCw==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwC8BeBAp/kBQcLAE0HdhyVB+U43QQ==", "subType":"09"}},
 * "legacyDoubleVector":[7.0007,8.12345,9.23456,10.34567,11.45678], "year":2023}},
 * {"_id":8, "int8Vector":{"$binary":{"base64":"AwAICQoLDA==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwBHAwBBp/kRQcLAI0HdhzVB+U5HQQ==", "subType":"09"}},
 * "legacyDoubleVector":[8.0008,9.12345,10.23456,11.34567,12.45678], "year":2024}},
 * {"_id":9, "int8Vector":{"$binary":{"base64":"AwAJCgsMDQ==", "subType":"09"}},
 * "float32Vector":{"$binary":{"base64":"JwCwAxBBp/khQcLAM0Hdh0VB+U5XQQ==", "subType":"09"}},
 * "legacyDoubleVector":[9.0009,10.12345,11.23456,12.34567,13.45678], "year":2025}]
 * </code>
 */
class AggregatesBinaryVectorSearchIntegrationTest {
    private static final MongoNamespace BINARY_VECTOR_NAMESPACE = new MongoNamespace("javaExtraTests", "binaryVectorTests");
    private static final String VECTOR_INDEX = "vector_search_index";
    private static final String VECTOR_FIELD_INT_8 = "int8Vector";
    private static final String VECTOR_FIELD_FLOAT_32 = "float32Vector";
    private static final String VECTOR_FIELD_LEGACY_DOUBLE_LIST = "legacyDoubleVector";
    private static final int LIMIT = 5;
    private static CollectionHelper<Document> collectionHelper;


    @BeforeAll
    static void beforeAll() {
        assumeTrue(isAtlasSearchTest());
        assumeTrue(serverVersionAtLeast(6, 0));

        collectionHelper = new CollectionHelper<>(new DocumentCodec(), BINARY_VECTOR_NAMESPACE);
    }

    private static Stream<Arguments> provideSupportedVectors() {
        return Stream.of(
                arguments(BinaryVector.int8Vector(new byte[]{0, 1, 2, 3, 4}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_INT_8).multi("ignored"),
                        approximateVectorSearchOptions(LIMIT * 2)),
                arguments(BinaryVector.int8Vector(new byte[]{0, 1, 2, 3, 4}),
                        fieldPath(VECTOR_FIELD_INT_8),
                        approximateVectorSearchOptions(LIMIT * 2)),

                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_FLOAT_32).multi("ignored"),
                        approximateVectorSearchOptions(LIMIT * 2)),
                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_FLOAT_32),
                        approximateVectorSearchOptions(LIMIT * 2)),

                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_FLOAT_32).multi("ignored"),
                        exactVectorSearchOptions()),
                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_FLOAT_32),
                        exactVectorSearchOptions()),

                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST).multi("ignored"),
                        exactVectorSearchOptions()),
                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST),
                        exactVectorSearchOptions()),

                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        // `multi` is used here only to verify that it is tolerated
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST).multi("ignored"),
                        approximateVectorSearchOptions(LIMIT * 2)),
                arguments(BinaryVector.floatVector(new float[]{0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f}),
                        fieldPath(VECTOR_FIELD_LEGACY_DOUBLE_LIST),
                        approximateVectorSearchOptions(LIMIT * 2))
        );
    }

    @ParameterizedTest
    @MethodSource("provideSupportedVectors")
    void shouldSearchByVectorWithSearchScore(final BinaryVector vector,
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
    void shouldSearchByVector(final BinaryVector vector,
                                       final FieldSearchPath fieldSearchPath,
                                       final VectorSearchOptions vectorSearchOptions) {
        //given
        List<Bson> pipeline = singletonList(
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
    void shouldSearchByVectorWithFilter(final BinaryVector vector,
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

}

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

package com.mongodb.client.model;

import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.client.model.mql.MqlValues;

import static com.mongodb.client.model.search.VectorSearchOptions.exactVectorSearchOptions;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.Accumulators.median;
import static com.mongodb.client.model.Accumulators.percentile;
import static com.mongodb.client.model.Aggregates.geoNear;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.rerank;
import static com.mongodb.client.model.Aggregates.scoreFusion;
import static com.mongodb.client.model.Aggregates.score;
import static com.mongodb.client.model.Aggregates.unset;
import static com.mongodb.client.model.Aggregates.vectorSearch;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.RerankQuery.rerankQuery;
import static com.mongodb.client.model.GeoNearOptions.geoNearOptions;
import static com.mongodb.client.model.ScoreFusionOptions.scoreFusionOptions;
import static com.mongodb.client.model.ScoreOptions.scoreOptions;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Windows.Bound.UNBOUNDED;
import static com.mongodb.client.model.Windows.documents;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchQuery.textQuery;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AggregatesTest extends OperationTest {

    private static final Bson SCORE_BY_X = score("$x");
    private static final Bson SCORE_BY_Y = score("$y");

    private static Stream<Arguments> groupWithQuantileSource() {
        return Stream.of(
                Arguments.of(percentile("result", "$x", MqlValues.ofNumberArray(0.95), QuantileMethod.approximate()), asList(3.0), asList(1.0)),
                Arguments.of(percentile("result", "$x", MqlValues.ofNumberArray(0.95, 0.3), QuantileMethod.approximate()), asList(3.0, 2.0), asList(1.0, 1.0)),
                Arguments.of(median("result", "$x", QuantileMethod.approximate()), 2.0d, 1.0d)
        );
    }

    @ParameterizedTest
    @MethodSource("groupWithQuantileSource")
    public void shouldGroupWithQuantile(final BsonField quantileAccumulator,
            final Object expectedGroup1,
            final Object expectedGroup2) {
        //given
        assumeTrue(serverVersionAtLeast(7, 0));
        getCollectionHelper().insertDocuments("[\n"
                + "   { _id: 1, x: 1, z: false},\n"
                + "   { _id: 2, x: 2, z: true },\n"
                + "   { _id: 3, x: 3, z: true }\n"
                + "]");

        //when
        List<Document> results = getCollectionHelper().aggregate(Collections.singletonList(
                group("$z", quantileAccumulator)), DOCUMENT_DECODER);

        //then
        assertThat(results, hasSize(2));

        Object result = results.stream()
                .filter(document -> document.get("_id").equals(true))
                .findFirst().map(document -> document.get("result")).get();


        assertEquals(expectedGroup1, result);

        result = results.stream()
                .filter(document -> document.get("_id").equals(false))
                .findFirst().map(document -> document.get("result")).get();

        assertEquals(expectedGroup2, result);
    }

    private static Stream<Arguments> setWindowFieldWithQuantileSource() {
        return Stream.of(
                Arguments.of(null,
                        WindowOutputFields.percentile("result", "$num1", asList(0.1, 0.9), QuantileMethod.approximate(),
                                documents(UNBOUNDED, UNBOUNDED)),
                        asList(asList(1.0, 3.0), asList(1.0, 3.0), asList(1.0, 3.0))),
                Arguments.of("$partitionId",
                        WindowOutputFields.percentile("result", "$num1", asList(0.1, 0.9), QuantileMethod.approximate(), null),
                        asList(asList(1.0, 2.0), asList(1.0, 2.0), asList(3.0, 3.0))),
                Arguments.of(null,
                        WindowOutputFields.median("result", "$num1", QuantileMethod.approximate(), documents(UNBOUNDED, UNBOUNDED)),
                        asList(2.0, 2.0, 2.0)),
                Arguments.of("$partitionId",
                        WindowOutputFields.median("result", "$num1", QuantileMethod.approximate(), null),
                        asList(1.0, 1.0, 3.0))
        );
    }

    @ParameterizedTest
    @MethodSource("setWindowFieldWithQuantileSource")
    public void shouldSetWindowFieldWithQuantile(@Nullable final Object partitionBy,
            final WindowOutputField output,
            final List<Object> expectedFieldValues) {
        //given
        assumeTrue(serverVersionAtLeast(7, 0));
        Document[] original = new Document[]{
                new Document("partitionId", 1).append("num1", 1),
                new Document("partitionId", 1).append("num1", 2),
                new Document("partitionId", 2).append("num1", 3)
        };
        getCollectionHelper().insertDocuments(original);

        //when
        List<Object> actualFieldValues = aggregateWithWindowFields(partitionBy, output, ascending("num1"));

        //then
        Assertions.assertEquals(actualFieldValues, expectedFieldValues);
    }

    @Test
    public void testUnset() {
        getCollectionHelper().insertDocuments("[\n"
                + "   { _id: 1, title: 'Antelope Antics', author: { last:'An', first: 'Auntie' } },\n"
                + "   { _id: 2, title: 'Bees Babble', author: { last:'Bumble', first: 'Bee' } }\n"
                + "]");

        assertPipeline(
                "{ $unset: ['title', 'author.first'] }",
                unset("title", "author.first"));

        List<Bson> pipeline = assertPipeline(
                "{ $unset: 'author.first' }",
                unset("author.first"));

        assertResults(pipeline, "[\n"
                + "   { _id: 1, title: 'Antelope Antics', author: { last:'An' } },\n"
                + "   { _id: 2, title: 'Bees Babble', author: { last:'Bumble' } }\n"
                + "]");

        assertPipeline(
                "{ $unset: ['title', 'author.first'] }",
                unset(asList("title", "author.first")));

        assertPipeline(
                "{ $unset: 'author.first' }",
                unset(asList("author.first")));
    }

    @Test
    public void testGeoNear() {
        getCollectionHelper().insertDocuments("[\n"
                + "   {\n"
                + "      _id: 1,\n"
                + "      name: 'Central Park',\n"
                + "      location: { type: 'Point', coordinates: [ -73.97, 40.77 ] },\n"
                + "      category: 'Parks'\n"
                + "   },\n"
                + "   {\n"
                + "      _id: 2,\n"
                + "      name: 'Sara D. Roosevelt Park',\n"
                + "      location: { type: 'Point', coordinates: [ -73.9928, 40.7193 ] },\n"
                + "      category: 'Parks'\n"
                + "   },\n"
                + "   {\n"
                + "      _id: 3,\n"
                + "      name: 'Polo Grounds',\n"
                + "      location: { type: 'Point', coordinates: [ -73.9375, 40.8303 ] },\n"
                + "      category: 'Stadiums'\n"
                + "   }\n"
                + "]");
        getCollectionHelper().createIndex(BsonDocument.parse("{ location: '2dsphere' }"));

        assertPipeline("{\n"
                        + "   $geoNear: {\n"
                        + "      near: { type: 'Point', coordinates: [ -73.99279 , 40.719296 ] },\n"
                        + "      distanceField: 'dist.calculated'\n"
                        + "   }\n"
                        + "}",
                geoNear(
                        new Point(new Position(-73.99279, 40.719296)),
                        "dist.calculated"
                ));

        List<Bson> pipeline = assertPipeline("{\n"
                        + "   $geoNear: {\n"
                        + "      near: { type: 'Point', coordinates: [ -73.99279 , 40.719296 ] },\n"
                        + "      distanceField: 'dist.calculated',\n"
                        + "      minDistance: 0,\n"
                        + "      maxDistance: 2,\n"
                        + "      query: { category: 'Parks' },\n"
                        + "      includeLocs: 'dist.location',\n"
                        + "      spherical: true,\n"
                        + "      key: 'location',\n"
                        + "      distanceMultiplier: 10.0\n"
                        + "   }\n"
                        + "}",
                geoNear(
                        new Point(new Position(-73.99279, 40.719296)),
                        "dist.calculated",
                        geoNearOptions()
                                .minDistance(0)
                                .maxDistance(2)
                                .query(new Document("category", "Parks"))
                                .includeLocs("dist.location")
                                .spherical()
                                .key("location")
                                .distanceMultiplier(10.0)
                ));

        assertResults(pipeline, ""
                + "[{\n"
                + "   '_id': 2,\n"
                + "   'name' : 'Sara D. Roosevelt Park',\n"
                + "   'category' : 'Parks',\n"
                + "   'location' : {\n"
                + "      'type' : 'Point',\n"
                + "      'coordinates' : [ -73.9928, 40.7193 ]\n"
                + "   },\n"
                + "   'dist' : {\n"
                + "      'calculated' : 9.5399,\n"
                + "      'location' : {\n"
                + "         'type' : 'Point',\n"
                + "         'coordinates' : [ -73.9928, 40.7193 ]\n"
                + "      }\n"
                + "   }\n"
                + "}]", 4, RoundingMode.FLOOR);
    }

    @Test
    public void testDocuments() {
        assumeTrue(serverVersionAtLeast(5, 1));
        Bson stage = Aggregates.documents(asList(
                Document.parse("{a: 1, b: {$add: [1, 1]} }"),
                BsonDocument.parse("{a: 3, b: 4}")));
        assertPipeline(
                "{$documents: [{a: 1, b: {$add: [1, 1]}}, {a: 3, b: 4}]}",
                stage);

        List<Bson> pipeline = asList(stage);
        getCollectionHelper().aggregateDb(pipeline);

        assertEquals(
                parseToList("[{a: 1, b: 2}, {a: 3, b: 4}]"),
                getCollectionHelper().aggregateDb(pipeline));

        // accepts lists of Documents and BsonDocuments
        List<BsonDocument> documents = asList(BsonDocument.parse("{a: 1, b: 2}"));
        assertPipeline("{$documents: [{a: 1, b: 2}]}", Aggregates.documents(documents));
        List<BsonDocument> bsonDocuments = asList(BsonDocument.parse("{a: 1, b: 2}"));
        assertPipeline("{$documents: [{a: 1, b: 2}]}", Aggregates.documents(bsonDocuments));
    }

    @Test
    public void testDocumentsLookup() {
        assumeTrue(serverVersionAtLeast(5, 1));

        getCollectionHelper().insertDocuments("[{_id: 1, a: 8}, {_id: 2, a: 9}]");
        Bson documentsStage = Aggregates.documents(asList(Document.parse("{a: 5}")));

        Bson lookupStage = Aggregates.lookup(null, asList(documentsStage), "added");
        assertPipeline(
                "{'$lookup': {'pipeline': [{'$documents': [{'a': 5}]}], 'as': 'added'}}",
                lookupStage);
        assertEquals(
                parseToList("[{_id:1, a:8, added: [{a: 5}]}, {_id:2, a:9, added: [{a: 5}]}]"),
                getCollectionHelper().aggregate(asList(lookupStage)));
    }

    @Test
    public void testAprVectorSearchWithQueryObject() {
        assertPipeline(
                "{"
                        + "  $vectorSearch: {"
                        + "    path: 'plot',"
                        + "    query: {text: 'movies about love'},"
                        + "    index: 'test_index',"
                        + "    limit: {$numberLong: '5'},"
                        + "    numCandidates: {$numberLong: '5'}"
                        + "  }"
                        + "}",
                vectorSearch(
                        fieldPath("plot"),
                        textQuery("movies about love"),
                        "test_index",
                        5L,
                        approximateVectorSearchOptions(5L)
                ));
    }

    @Test
    public void testAprVectorSearchWithQueryObjectAndEmbeddingModel() {
        assertPipeline(
                "{"
                        + "  $vectorSearch: {"
                        + "    path: 'plot',"
                        + "    query: {text: 'movies about love'},"
                        + "    index: 'test_index',"
                        + "    limit: {$numberLong: '5'},"
                        + "    model: 'voyage-4-large',"
                        + "    numCandidates: {$numberLong: '5'}"
                        + "  }"
                        + "}",
                vectorSearch(
                        fieldPath("plot"),
                        textQuery("movies about love").model("voyage-4-large"),
                        "test_index",
                        5L,
                        approximateVectorSearchOptions(5L)
                ));
    }

    @Test
    public void testExactVectorSearchWithQueryObjectAndEmbeddingModel() {
        assertPipeline(
                "{"
                        + "  $vectorSearch: {"
                        + "    path: 'plot',"
                        + "    query: {text: 'movies about love'},"
                        + "    index: 'test_index',"
                        + "    limit: {$numberLong: '5'},"
                        + "    model: 'voyage-4-large',"
                        + "    exact: true"
                        + "  }"
                        + "}",
                vectorSearch(
                        fieldPath("plot"),
                        textQuery("movies about love").model("voyage-4-large"),
                        "test_index",
                        5L,
                        exactVectorSearchOptions()
                ));
    }
    @Test
    public void testExactVectorSearchWithQueryObject() {
        assertPipeline(
                "{"
                        + "  $vectorSearch: {"
                        + "    path: 'plot',"
                        + "    query: {text: 'movies about love'},"
                        + "    index: 'test_index',"
                        + "    limit: {$numberLong: '5'},"
                        + "    exact: true"
                        + "  }"
                        + "}",
                vectorSearch(
                        fieldPath("plot"),
                        textQuery("movies about love"),
                        "test_index",
                        5L,
                        exactVectorSearchOptions()
                ));
    }

    @Test
    public void testRerankWithSinglePath() {
        assertPipeline(
                "{"
                        + "  '$rerank': {"
                        + "    'query': {'text': 'machine learning tutorials'},"
                        + "    'path': 'content',"
                        + "    'numDocsToRerank': 25,"
                        + "    'model': 'rerank-2.5'"
                        + "  }"
                        + "}",
                rerank(
                        rerankQuery("machine learning tutorials"),
                        "content",
                        25,
                        "rerank-2.5"
                ));
    }

    @Test
    public void testRerankWithMultiplePaths() {
        assertPipeline(
                "{"
                        + "  '$rerank': {"
                        + "    'query': {'text': 'machine learning tutorials'},"
                        + "    'path': ['content', 'title'],"
                        + "    'numDocsToRerank': 50,"
                        + "    'model': 'rerank-2.5-lite'"
                        + "  }"
                        + "}",
                rerank(
                        rerankQuery("machine learning tutorials"),
                        asList("content", "title"),
                        50,
                        "rerank-2.5-lite"
                ));
    }

    @Test
    public void testRerankWithBsonQuery() {
        assertPipeline(
                "{"
                        + "  '$rerank': {"
                        + "    'query': {'text': 'machine learning tutorials', 'imageURL': 'https://example.com/img.png'},"
                        + "    'path': 'content',"
                        + "    'numDocsToRerank': 25,"
                        + "    'model': 'rerank-2.5'"
                        + "  }"
                        + "}",
                rerank(
                        rerankQuery(new Document("text", "machine learning tutorials")
                                .append("imageURL", "https://example.com/img.png")),
                        "content",
                        25,
                        "rerank-2.5"
                ));
    }

    @Test
    public void testRerankWithMultiplePathsAndBsonQuery() {
        assertPipeline(
                "{"
                        + "  '$rerank': {"
                        + "    'query': {'text': 'machine learning tutorials', 'imageURL': 'https://example.com/img.png'},"
                        + "    'path': ['content', 'title'],"
                        + "    'numDocsToRerank': 100,"
                        + "    'model': 'rerank-2'"
                        + "  }"
                        + "}",
                rerank(
                        rerankQuery(new Document("text", "machine learning tutorials")
                                .append("imageURL", "https://example.com/img.png")),
                        asList("content", "title"),
                        100,
                        "rerank-2"
                ));
    }

    @Test
    public void testScoreWithExpression() {
        assertPipeline(
                "{'$score': {'score': {'$multiply': ['$rating', 2]}}}",
                score(new Document("$multiply", asList("$rating", 2))));
    }

    @Test
    public void testScoreWithAllOptions() {
        assertPipeline(
                "{"
                        + "  '$score': {"
                        + "    'score': '$rating',"
                        + "    'normalization': 'sigmoid',"
                        + "    'weight': 0.5,"
                        + "    'scoreDetails': true"
                        + "  }"
                        + "}",
                score("$rating", scoreOptions()
                        .normalization(ScoreNormalization.SIGMOID)
                        .weight(0.5)
                        .scoreDetails(true)));
    }

    @ParameterizedTest
    @EnumSource(ScoreNormalization.class)
    public void testScoreFusionWithEachNormalization(final ScoreNormalization normalization) {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        List<BsonDocument> results = resultsWithScoresFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                normalization));
        assertEquals(3, results.size());
        if (normalization == ScoreNormalization.NONE) {
            // the server combines the scores of the two pipelines with avg by default: (x + y) / 2
            // https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/
            assertEquals(asList(5.5, 3.5, 2.0), scoresFor(results));
        } else {
            // sigmoid maps each pipeline score into (0, 1) and minMaxScaler into [0, 1],
            // so the average of the two is within [0, 1]
            scoresFor(results).forEach(score -> Assertions.assertTrue(score >= 0 && score <= 1));
        }
    }

    @Test
    public void testScoreFusionWithWeights() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        // weight only the "byY" pipeline: expected order is descending y, each score is y / 2
        // because the server combines the weighted scores of the two pipelines with avg by default
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/
        List<BsonDocument> results = resultsWithScoresFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                ScoreNormalization.NONE,
                scoreFusionOptions().combination(
                        ScoreFusionCombination.weighted(new Document("byX", 0).append("byY", 1)))));
        assertEquals(asList(3, 2, 1), getIdsFor(results));
        assertEquals(asList(1.5, 1.0, 0.5), scoresFor(results));
    }

    @Test
    public void testScoreFusionWithWeightsAndAvgMethod() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        // weight only the "byX" pipeline and average over the two pipelines: each score is x / 2
        List<BsonDocument> results = resultsWithScoresFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                ScoreNormalization.NONE,
                scoreFusionOptions().combination(
                        ScoreFusionCombination.weighted(new Document("byX", 1).append("byY", 0)).avg())));
        assertEquals(asList(1, 2, 3), getIdsFor(results));
        assertEquals(asList(5.0, 2.5, 0.5), scoresFor(results));
    }

    @Test
    public void testScoreFusionWithExpressionCombination() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        // ignore byX, use 10 * byY: expected order is descending y, each score is exactly 10 * y
        List<BsonDocument> results = resultsWithScoresFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                ScoreNormalization.NONE,
                scoreFusionOptions().combination(ScoreFusionCombination.expression(
                        new Document("$sum", asList(
                                new Document("$multiply", asList("$$byX", 0)),
                                new Document("$multiply", asList("$$byY", 10))))))));
        assertEquals(asList(3, 2, 1), getIdsFor(results));
        assertEquals(asList(30.0, 20.0, 10.0), scoresFor(results));
    }

    @Test
    public void testScoreFusionWithScoreDetails() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        List<BsonDocument> results = getCollectionHelper().aggregate(asList(
                scoreFusion(
                        asList(
                                FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                                FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                        ScoreNormalization.SIGMOID,
                        scoreFusionOptions().scoreDetails(true)),
                project(Projections.fields(
                        Projections.meta("score", "score"),
                        Projections.meta("scoreDetails", "scoreDetails")))));
        assertEquals(3, results.size());
        results.forEach(result -> {
            double score = result.getNumber("score").doubleValue();
            Assertions.assertTrue(score > 0);
            BsonDocument scoreDetails = result.getDocument("scoreDetails");
            // "value" holds the same combined score as the score metadata
            assertEquals(score, scoreDetails.getNumber("value").doubleValue());
            Assertions.assertFalse(scoreDetails.getString("description").getValue().isEmpty());
            assertEquals("sigmoid", scoreDetails.getString("normalization").getValue());
            Assertions.assertNotNull(scoreDetails.getDocument("combination"));
            // one entry per input pipeline
            assertEquals(2, scoreDetails.getArray("details").size());
        });
    }

    @ParameterizedTest
    @EnumSource(ScoreNormalization.class)
    public void testScoreWithEachNormalization(final ScoreNormalization normalization) {
        assertPipeline(
                "{'$score': {'score': '$rating', 'normalization': '" + normalization.getValue() + "'}}",
                score("$rating", scoreOptions().normalization(normalization)));
    }

    @Test
    public void testScoreWeightValidation() {
        assertThrows(IllegalArgumentException.class, () -> scoreOptions().weight(-0.1));
        assertThrows(IllegalArgumentException.class, () -> scoreOptions().weight(1.1));
        assertThrows(IllegalArgumentException.class, () -> scoreOptions().weight(Double.NaN));
        assertPipeline(
                "{'$score': {'score': '$rating', 'weight': 0.0}}",
                score("$rating", scoreOptions().weight(0)));
        assertPipeline(
                "{'$score': {'score': '$rating', 'weight': 1.0}}",
                score("$rating", scoreOptions().weight(1)));
    }

    @Test
    public void testScore() {
        assumeTrue(serverVersionAtLeast(8, 2));
        getCollectionHelper().insertDocuments("[{_id: 1, rating: 2}, {_id: 2, rating: 4}]");

        List<Bson> pipeline = asList(
                score(new Document("$multiply", asList("$rating", 2)),
                        scoreOptions().normalization(ScoreNormalization.SIGMOID)),
                Aggregates.sort(ascending("_id")),
                Aggregates.project(Projections.computed("score", new Document("$meta", "score"))));

        List<BsonDocument> results = getCollectionHelper().aggregate(pipeline);
        assertEquals(2, results.size());
        // sigmoid normalization maps each score into the range (0, 1)
        results.forEach(result -> {
            double scoreValue = result.getNumber("score").doubleValue();
            Assertions.assertTrue(scoreValue > 0 && scoreValue < 1);
        });
    }

    @ParameterizedTest
    @EnumSource(ScoreNormalization.class)
    public void testScoreOnServerWithEachNormalization(final ScoreNormalization normalization) {
        assumeTrue(serverVersionAtLeast(8, 2));
        getCollectionHelper().insertDocuments("[{_id: 1, rating: 2}, {_id: 2, rating: 4}]");

        List<Bson> pipeline = asList(
                score("$rating", scoreOptions().normalization(normalization)),
                Aggregates.project(Projections.computed("score", new Document("$meta", "score"))));

        List<BsonDocument> results = getCollectionHelper().aggregate(pipeline);
        assertEquals(2, results.size());
        results.forEach(result -> Assertions.assertTrue(result.isNumber("score")));
    }

    private void insertScoreFusionDocuments() {
        getCollectionHelper().insertDocuments(
                BsonDocument.parse("{_id: 1, x: 10, y: 1}"),
                BsonDocument.parse("{_id: 2, x: 5, y: 2}"),
                BsonDocument.parse("{_id: 3, x: 1, y: 3}"));
    }

    private List<BsonDocument> resultsWithScoresFor(final Bson scoreFusionStage) {
        return getCollectionHelper().aggregate(asList(
                scoreFusionStage,
                project(Projections.meta("score", "score"))));
    }

    private List<Integer> getIdsFor(final List<BsonDocument> results) {
        return results.stream()
                .map(doc -> doc.getInt32("_id").getValue())
                .collect(toList());
    }

    private List<Double> scoresFor(final List<BsonDocument> results) {
        return results.stream()
                .map(doc -> doc.getNumber("score").doubleValue())
                .collect(toList());
    }
}

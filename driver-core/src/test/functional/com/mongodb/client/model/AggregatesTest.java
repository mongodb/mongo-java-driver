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
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.Accumulators.median;
import static com.mongodb.client.model.Accumulators.percentile;
import static com.mongodb.client.model.Aggregates.geoNear;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.setWindowFields;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unset;
import static com.mongodb.client.model.GeoNearOptions.geoNearOptions;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Windows.Bound.UNBOUNDED;
import static com.mongodb.client.model.Windows.documents;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AggregatesTest extends OperationTest {

    private static Stream<Arguments> shouldGroupWithPercentile() {
        return Stream.of(
                Arguments.of(new double[]{0.95}, asList(3.0), asList(1.0)),
                Arguments.of(new double[]{0.95, 0.3}, asList(3.0, 2.0), asList(1.0, 1.0))
        );
    }
    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("unchecked")
    public void shouldGroupWithPercentile(final double[] quantiles, final List<Double> expectedGroup1, final List<Double> expectedGroup2) {
        //given
        assumeTrue(serverVersionAtLeast(7, 0));
        getCollectionHelper().insertDocuments("[\n"
                + "   { _id: 1, x: 1, z: false },\n"
                + "   { _id: 2, x: 2, z: true },\n"
                + "   { _id: 3, x: 3, z: true },\n"
                + "]");
        //when
        List<Document> results = getCollectionHelper().aggregate(Collections.singletonList(
                group(new Document("gid", "$z"),
                        percentile("sat_95", "$x", quantiles, "approximate"))), new DocumentCodec());
        //then
        assertThat(results, hasSize(2));

        List<Double> result = results.stream()
                .filter(document -> document.get("_id").equals(new Document("gid", true)))
                .findFirst().map(document -> document.get("sat_95", List.class)).get();

        assertEquals(expectedGroup1, result);

        result = results.stream()
                .filter(document -> document.get("_id").equals(new Document("gid", false)))
                .findFirst().map(document -> document.get("sat_95", List.class)).get();

        assertEquals(expectedGroup2, result);
    }

    @Test
    public void shouldGroupWithMedian() {
        //given
        assumeTrue(serverVersionAtLeast(7, 0));
        getCollectionHelper().insertDocuments("[\n"
                + "   { _id: 1, x: 1, z: false },\n"
                + "   { _id: 2, x: 2, z: true },\n"
                + "   { _id: 3, x: 3, z: true },\n"
                + "]");

        //when
        List<Document> results = getCollectionHelper().aggregate(Collections.singletonList(
                group(new Document("gid", "$z"),
                        median("sat_95", "$x", "approximate"))), new DocumentCodec());

        //then
        assertThat(results, hasSize(2));

        Double result = results.stream()
                .filter(document -> document.get("_id").equals(new Document("gid", true)))
                .findFirst().map(document -> document.get("sat_95", Double.class)).get();

        assertEquals(2.0, result);

        result = results.stream()
                .filter(document -> document.get("_id").equals(new Document("gid", false)))
                .findFirst().map(document -> document.get("sat_95", Double.class)).get();

        assertEquals(1.0, result);
    }

    private static Stream<Arguments> shouldSetWindowFieldWithQuantiles() {
        return Stream.of(
                Arguments.of(null,
                        WindowOutputFields.percentile("result", "$num1", new double[]{0.1, 0.9}, "approximate", documents(UNBOUNDED, UNBOUNDED)),
                        asList(asList(1.0, 3.0), asList(1.0, 3.0), asList(1.0, 3.0))),
                Arguments.of("$partitionId",
                        WindowOutputFields.percentile("result", "$num1", new double[]{0.1, 0.9}, "approximate", null),
                        asList(asList(1.0, 2.0), asList(1.0, 2.0), asList(3.0, 3.0))),
                Arguments.of(null,
                        WindowOutputFields.median("result", "$num1", "approximate", documents(UNBOUNDED, UNBOUNDED)),
                        asList(2.0, 2.0, 2.0)),
                Arguments.of("$partitionId",
                        WindowOutputFields.median("result", "$num1", "approximate", null),
                        asList(1.0, 1.0, 3.0))
        );
    }
    @ParameterizedTest
    @MethodSource
    public void shouldSetWindowFieldWithQuantiles(final Object partitionBy,
                                                  final WindowOutputField output, final List<Object> expectedFieldValues){
        //given
        assumeTrue(serverVersionAtLeast(7, 0));
        ZoneId utc = ZoneId.of(ZoneOffset.UTC.getId());
        Document[] original = new Document[]{
                new Document("partitionId", 1)
                        .append("num1", 1)
                        .append("num2", -1)
                        .append("numMissing", 1)
                        .append("date", LocalDateTime.ofInstant(Instant.ofEpochSecond(1), utc)),
                new Document("partitionId", 1)
                        .append("num1", 2)
                        .append("num2", -2)
                        .append("date", LocalDateTime.ofInstant(Instant.ofEpochSecond(2), utc)),
                new Document("partitionId", 2)
                        .append("num1", 3)
                        .append("num2", -3)
                        .append("numMissing", 3)
                        .append("date", LocalDateTime.ofInstant(Instant.ofEpochSecond(3), utc))};
        getCollectionHelper().insertDocuments(original);

        //when
        List<Object> actualFieldValues = aggregateWithWindowFields(partitionBy, output);

        //then
        Assertions.assertEquals(actualFieldValues, expectedFieldValues);
    }

    @Test
    public void testUnset() {
        assumeTrue(serverVersionAtLeast(4, 2));
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
                + "      'calculated' : 9.539931676365992,\n"
                + "      'location' : {\n"
                + "         'type' : 'Point',\n"
                + "         'coordinates' : [ -73.9928, 40.7193 ]\n"
                + "      }\n"
                + "   }\n"
                + "}]");
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

        List<Bson> pipeline = Arrays.asList(stage);
        getCollectionHelper().aggregateDb(pipeline);

        assertEquals(
                parseToList("[{a: 1, b: 2}, {a: 3, b: 4}]"),
                getCollectionHelper().aggregateDb(pipeline));

        // accepts lists of Documents and BsonDocuments
        List<BsonDocument> documents = Arrays.asList(BsonDocument.parse("{a: 1, b: 2}"));
        assertPipeline("{$documents: [{a: 1, b: 2}]}", Aggregates.documents(documents));
        List<BsonDocument> bsonDocuments = Arrays.asList(BsonDocument.parse("{a: 1, b: 2}"));
        assertPipeline("{$documents: [{a: 1, b: 2}]}", Aggregates.documents(bsonDocuments));
    }

    @Test
    public void testDocumentsLookup() {
        assumeTrue(serverVersionAtLeast(5, 1));

        getCollectionHelper().insertDocuments("[{_id: 1, a: 8}, {_id: 2, a: 9}]");
        Bson documentsStage = Aggregates.documents(asList(Document.parse("{a: 5}")));

        Bson lookupStage = Aggregates.lookup("ignored", Arrays.asList(documentsStage), "added");
        assertPipeline(
                "{'$lookup': {'from': 'ignored', 'pipeline': [{'$documents': [{'a': 5}]}], 'as': 'added'}}",
                lookupStage);
        assertEquals(
                parseToList("[{_id:1, a:8, added: [{a: 5}]}, {_id:2, a:9, added: [{a: 5}]}]"),
                getCollectionHelper().aggregate(Arrays.asList(lookupStage)));

        // null variant
        Bson lookupStageNull = Aggregates.lookup(null, Arrays.asList(documentsStage), "added");
        assertPipeline(
                "{'$lookup': {'pipeline': [{'$documents': [{'a': 5}]}], 'as': 'added'}}",
                lookupStageNull);
        assertEquals(
                parseToList("[{_id:1, a:8, added: [{a: 5}]}, {_id:2, a:9, added: [{a: 5}]}]"),
                getCollectionHelper().aggregate(Arrays.asList(lookupStageNull)));
    }

    private List<Object> aggregateWithWindowFields(final Object partitionBy, final WindowOutputField output) {
        List<Bson> stages = new ArrayList<>();
        stages.add(setWindowFields(partitionBy, null, output));
        stages.add(sort(ascending("num1")));

        List<Document> actual = getCollectionHelper().aggregate(stages, new DocumentCodec());

        return actual.stream()
                .map(doc -> doc.get("result"))
                .collect(toList());
    }
}

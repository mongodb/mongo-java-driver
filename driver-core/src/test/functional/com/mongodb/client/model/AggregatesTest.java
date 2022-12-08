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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.GeoNearOptions.geoNearOptions;
import static com.mongodb.client.model.Aggregates.geoNear;
import static com.mongodb.client.model.Aggregates.unset;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AggregatesTest extends OperationTest {

    private List<Bson> assertPipeline(final String stageAsString, final Bson stage) {
        BsonDocument expectedStage = BsonDocument.parse(stageAsString);
        List<Bson> pipeline = Collections.singletonList(stage);
        assertEquals(expectedStage, pipeline.get(0).toBsonDocument(BsonDocument.class, getDefaultCodecRegistry()));
        return pipeline;
    }

    private void assertResults(final List<Bson> pipeline, final String s) {
        List<Document> expectedResults = parseToList(s);
        List<Document> results = getCollectionHelper().aggregate(pipeline);
        assertEquals(expectedResults, results);
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
        Bson lookupStageNull = Aggregates.lookup(Arrays.asList(documentsStage), "added");
        assertPipeline(
                "{'$lookup': {'pipeline': [{'$documents': [{'a': 5}]}], 'as': 'added'}}",
                lookupStageNull);
        assertEquals(
                parseToList("[{_id:1, a:8, added: [{a: 5}]}, {_id:2, a:9, added: [{a: 5}]}]"),
                getCollectionHelper().aggregate(Arrays.asList(lookupStageNull)));
    }
}

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

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.search.SearchScore;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.connection.Cluster;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.createCluster;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.search;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.search.SearchOperators.text;
import static com.mongodb.client.model.search.SearchPath.path;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class SearchOperatorsIntegrationTest {
    private static Cluster cluster;
    private static CollectionHelper<BsonDocument> collectionHelper;

    @BeforeAll
    public static void beforeAll() {
        assumeTrue(System.getProperties().containsKey("org.mongodb.test.search.uri"));
        cluster = createCluster(new ConnectionString(System.getProperty("org.mongodb.test.search.uri")));
        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), cluster,
                new MongoNamespace("sample_mflix", "movies"));
    }

    @AfterAll
    public static void afterAll() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void testTextOperator() {
        List<Bson> pipeline = asList(
                search(text("cowboy", path("title")).score(SearchScore.boost(1.5))),
                project(fields(excludeId(), include("title"))));
        List<BsonDocument> results = collectionHelper.aggregate(pipeline);

        assertEquals(new HashSet<>(Arrays.asList("Cowboy del Amor", "Cowboy Bebop: The Movie", "Midnight Cowboy", "Cowboy",
                        "Last Cowboy Standing", "The Cowboy and the Lady", "Urban Cowboy", "Drugstore Cowboy")),
                results.stream().map((value) -> value.getString("title").getValue()).collect(Collectors.toSet()));
    }
}

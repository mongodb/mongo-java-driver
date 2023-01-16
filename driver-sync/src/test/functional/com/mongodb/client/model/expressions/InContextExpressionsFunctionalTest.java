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

package com.mongodb.client.model.expressions;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.expressions.Expressions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InContextExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {

    private MongoClient client;
    private MongoCollection<Document> col;

    @BeforeEach
    public void setUp() {
        client = MongoClients.create();
        col = client.getDatabase("testdb").getCollection("testcol");
        col.drop();
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    private static String bsonToString(final Bson project) {
        return project.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).toString().replaceAll("\"", "'");
    }

    private List<Document> aggregate(final Bson... stages) {
        AggregateIterable<Document> result = col.aggregate(Arrays.asList(stages));
        List<Document> results = new ArrayList<>();
        result.forEach(r -> results.add(r));
        return results;
    }

    @Test
    public void findTest() {
        col.insertMany(Arrays.asList(
                Document.parse("{_id: 1, x: 0, y: 2}"),
                Document.parse("{_id: 2, x: 0, y: 3}"),
                Document.parse("{_id: 3, x: 1, y: 3}")));

        FindIterable<Document> iterable = col.find(expr(
                current().getInteger("x").eq(of(1))));
        List<Document> results = new ArrayList<>();
        iterable.forEach(r -> results.add(r));

        assertEquals(
                Arrays.asList(Document.parse("{_id: 3, x: 1, y: 3}")),
                results);
    }

    @Test
    public void matchTest() {
        col.insertMany(Arrays.asList(
                Document.parse("{_id: 1, x: 0, y: 2}"),
                Document.parse("{_id: 2, x: 0, y: 3}"),
                Document.parse("{_id: 3, x: 1, y: 3}")));

        List<Document> results = aggregate(
                match(expr(current().getInteger("x").eq(of(1)))));

        assertEquals(
                Arrays.asList(Document.parse("{_id: 3, x: 1, y: 3}")),
                results);
    }

    @Test
    public void currentAsMapMatchTest() {
        col.insertMany(Arrays.asList(
                Document.parse("{_id: 1, x: 0, y: 2}"),
                Document.parse("{_id: 2, x: 0, y: 3}"),
                Document.parse("{_id: 3, x: 1, y: 3}")));

        List<Document> results = aggregate(
                match(expr(Expressions.<NumberExpression>currentAsMap()
                        .entrySet()
                        .map(e -> e.getValue())
                        .sum(v -> v).eq(of(7)))));

        assertEquals(
                Arrays.asList(Document.parse("{_id: 3, x: 1, y: 3}")),
                results);
    }

    @Test
    public void projectTest() {
        col.insertMany(Arrays.asList(
                Document.parse("{_id: 1, x: 0, y: 2}")));

        List<Document> expected = Arrays.asList(Document.parse("{_id: 1, x: 0, c: 2}"));

        // old, using "$y"
        Bson projectOld = project(fields(include("x"), computed("c",
                "$y")));
        assertEquals("{'$project': {'x': 1, 'c': '$y'}}", bsonToString(projectOld));
        assertEquals(expected,
                aggregate(projectOld));

        // new, using current() with add/subtract
        Bson projectNew = project(fields(include("x"), computed("c",
                current().getInteger("y").add(10).subtract(10))));
        assertEquals(
                "{'$project': {'x': 1, 'c': "
                        + "{'$subtract': [{'$add': [{'$getField': "
                        + "{'input': '$$CURRENT', 'field': 'y'}}, 10]}, 10]}}}",
                bsonToString(projectNew));
        assertEquals(expected,
                aggregate(projectNew));
    }

    @Test
    public void projectTest2() {
        col.insertMany(Arrays.asList(Document.parse("{_id: 0, x: 1}")));

        // new, nestedArray
        Bson projectNestedArray = project(fields(excludeId(), computed("nestedArray", ofArray(
                current().getInteger("x").max(of(4)),
                current().getInteger("x"),
                of(0), of(1), of(true), of(false)
        ))));
        assertEquals(
                Arrays.asList(Document.parse("{ nestedArray: [ 4, 1, 0, 1, true, false ] }")),
                aggregate(projectNestedArray));

        // new, document
        Bson projectDocument = project(fields(computed("nested",
                // the below is roughly: "{ x: {$max : ['$x', 4] }}"
                of(Document.parse("{x: 9}")).setField("x", current().getInteger("x").max(of(4)))
        )));
        assertEquals(
                Arrays.asList(Document.parse("{_id: 0, nested: { x: 4 } }")),
                aggregate(projectDocument));
    }

    @Test
    public void groupTest() {
        col.insertMany(Arrays.asList(
                Document.parse("{t: 0, a: 1}"),
                Document.parse("{t: 0, a: 2}"),
                Document.parse("{t: 1, a: 9}")));

        List<Document> results = aggregate(
                Aggregates.group(
                        current().getInteger("t").add(of(100)),
                        sum("sum", current().getInteger("a").add(1))),
                Aggregates.sort(ascending("_id")));
        assertEquals(
                Arrays.asList(
                        Document.parse("{_id: 100, sum: 5}"),
                        Document.parse("{_id: 101, sum: 10}")),
                results);
    }
}

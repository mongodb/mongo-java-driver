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

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofArray;
import static com.mongodb.client.model.expressions.Expressions.ofEntry;
import static com.mongodb.client.model.expressions.Expressions.ofMap;

class MapExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {

    private final MapExpression<IntegerExpression> mapKey123 = Expressions.<IntegerExpression>ofEmptyMap()
            .set("key", of(123));

    private final MapExpression<IntegerExpression> mapA1B2 = ofMap(Document.parse("{keyA: 1, keyB: 2}"));

    @Test
    public void literalsTest() {
        // map
        assertExpression(
                Document.parse("{key: 123}"),
                mapKey123,
                "{'$setField': {'field': 'key', 'input': {'$literal': {}}, 'value': 123}}");
        assertExpression(
                Document.parse("{keyA: 1, keyB: 2}"),
                ofMap(Document.parse("{keyA: 1, keyB: 2}")),
                "{'$literal': {'keyA': 1, 'keyB': 2}}");
        // entry
        assertExpression(
                Document.parse("{k: 'keyA', v: 1}"),
                ofEntry("keyA", of(1)));
    }

    @Test
    public void getSetMapTest() {
        // get
        assertExpression(
                123,
                mapKey123.get("key"));
        assertExpression(
                1,
                mapKey123.get("missing", of(1)));
        // set (map.put)
        assertExpression(
                BsonDocument.parse("{key: 123, b: 1}"),
                mapKey123.set("b", of(1)));
        // unset (delete)
        assertExpression(
                BsonDocument.parse("{}"),
                mapKey123.unset("key"));
    }

    @Test
    public void getSetEntryTest() {
        EntryExpression<IntegerExpression> entryA1 = ofEntry("keyA", of(1));
        assertExpression(
                Document.parse("{k: 'keyA', 'v': 33}"),
                entryA1.setValue(of(33)));
        assertExpression(
                Document.parse("{k: 'keyB', 'v': 1}"),
                entryA1.setKey(of("keyB")));
        assertExpression(
                Document.parse("{k: 'keyB', 'v': 1}"),
                entryA1.setKey("keyB"));
    }

    @Test
    public void buildMapTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayToObject/ (48)
        assertExpression(
                Document.parse("{'keyA': 1}"),
                ofArray(ofEntry("keyA", of(1))).asMap(v -> v),
                "{'$arrayToObject': [[{'$literal': {'k': 'keyA', 'v': 1}}]]}");
    }

    @Test
    public void entrySetTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/objectToArray/ (23)
        assertExpression(
                Arrays.asList(Document.parse("{'k': 'k1', 'v': 1}")),
                Expressions.<IntegerExpression>ofEmptyMap().set("k1", of(1)).entrySet(),
                "{'$objectToArray': {'$setField': "
                        + "{'field': 'k1', 'input': {'$literal': {}}, 'value': 1}}}");

        // key/value usage
        assertExpression(
                "keyA|keyB|",
                mapA1B2.entrySet().map(v -> v.getKey().concat(of("|"))).join(v -> v));
        assertExpression(
                23,
                mapA1B2.entrySet().map(v -> v.getValue().add(10)).sum(v -> v));

        // combined entrySet-buildMap usage
        assertExpression(
                Document.parse("{'keyA': 2, 'keyB': 3}"),
                mapA1B2
                        .entrySet()
                        .map(v -> v.setValue(v.getValue().add(1)))
                        .asMap(v -> v));
    }

    @Test
    public void mergeTest() {
        assertExpression(
                Document.parse("{'keyA': 9, 'keyB': 2, 'keyC': 3}"),
                ofMap(Document.parse("{keyA: 1, keyB: 2}"))
                        .merge(ofMap(Document.parse("{keyA: 9, keyC: 3}"))),
                "{'$mergeObjects': [{'$literal': {'keyA': 1, 'keyB': 2}}, "
                        + "{'$literal': {'keyA': 9, 'keyC': 3}}]}");
    }

}

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

package com.mongodb.client.model.mql;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.mql.MqlValues.of;
import static com.mongodb.client.model.mql.MqlValues.ofArray;
import static com.mongodb.client.model.mql.MqlValues.ofEntry;
import static com.mongodb.client.model.mql.MqlValues.ofMap;
import static com.mongodb.client.model.mql.MqlValues.ofStringArray;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MapMqlValuesFunctionalTest extends AbstractMqlValuesFunctionalTest {

    private final MqlMap<MqlInteger> mapKey123 = MqlValues.<MqlInteger>ofMap()
            .set("key", of(123));

    private final MqlMap<MqlInteger> mapA1B2 = ofMap(Document.parse("{keyA: 1, keyB: 2}"));

    @Test
    public void literalsTest() {
        // entry
        assertExpression(
                Document.parse("{k: 'keyA', v: 1}"),
                ofEntry(of("keyA"), of(1)));
        assumeTrue(serverVersionAtLeast(5, 0)); // get/setField (unset)
        // map
        assertExpression(
                Document.parse("{keyA: 1, keyB: 2}"),
                ofMap(Document.parse("{keyA: 1, keyB: 2}")),
                "{'$literal': {'keyA': 1, 'keyB': 2}}");
        assertExpression(
                Document.parse("{key: 123}"),
                mapKey123,
                "{'$setField': {'field': 'key', 'input': {'$literal': {}}, 'value': 123}}");
    }

    @Test
    public void getSetMapTest() {
        assumeTrue(serverVersionAtLeast(5, 0)); // get/setField
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
        // "other" parameter
        assertExpression(
                null,
                ofMap(Document.parse("{ 'null': null }")).get("null", of(1)));
    }

    @Test
    public void hasTest() {
        assumeTrue(serverVersionAtLeast(5, 0)); // get/setField (unset)
        MqlMap<MqlValue> e = ofMap(BsonDocument.parse("{key: 1, null: null}"));
        assertExpression(
                true,
                e.has(of("key")),
                "{'$ne': [{'$getField': {'input': {'$literal': {'key': 1, 'null': null}}, 'field': 'key'}}, '$$REMOVE']}");
        assertExpression(
                false,
                e.has("not_key"));
        assertExpression(
                true,
                e.has("null"));
        // consistency:
        assertExpression(true, e.has("null"));
        assertExpression(null, e.get("null", of(1)));
    }

    @Test
    public void getSetEntryTest() {
        assumeTrue(serverVersionAtLeast(5, 0)); // get/setField
        MqlEntry<MqlInteger> entryA1 = ofEntry(of("keyA"), of(1));
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
                ofArray(ofEntry(of("keyA"), of(1))).asMap(v -> v),
                "{'$arrayToObject': [{'$map': {'input': [{'k': 'keyA', 'v': 1}], 'in': '$$this'}}]}");

        assumeTrue(serverVersionAtLeast(5, 0)); // get/setField
        assertExpression(
                Document.parse("{'keyA': 55}"),
                ofArray(ofEntry(of("keyA"), of(1))).asMap(v -> v.setValue(of(55))),
                "{'$arrayToObject': [{'$map': {'input': [{'k': 'keyA', 'v': 1}], "
                        + "'in': {'$setField': {'field': 'v', 'input': '$$this', 'value': 55}}}}]}");

        // using documents
        assertExpression(
                Document.parse("{ 'item' : 'abc123', 'qty' : 25 }"),
                ofArray(
                        of(Document.parse("{ 'k': 'item', 'v': 'abc123' }")),
                        of(Document.parse("{ 'k': 'qty', 'v': 25 }")))
                        .asMap(v -> ofEntry(v.getString("k"), v.getField("v"))));
        // using arrays
        assertExpression(
                Document.parse("{ 'item' : 'abc123', 'qty' : 25 }"),
                ofArray(
                        ofStringArray("item", "abc123"),
                        ofArray(of("qty"), of(25)))
                        .asMap(v -> ofEntry(v.elementAt(of(0)).asString(), v.elementAt(of(1)))));
        // last listed value used
        assertExpression(
                Document.parse("{ 'item' : 'abc123' }"),
                ofArray(
                        MqlValues.<MqlString>ofMap(Document.parse("{ 'k': 'item', 'v': '123abc' }")),
                        MqlValues.<MqlString>ofMap(Document.parse("{ 'k': 'item', 'v': 'abc123' }")))
                        .asMap(v -> ofEntry(v.get("k"), v.get("v"))));

    }

    @Test
    public void entrySetTest() {
        assumeTrue(serverVersionAtLeast(5, 0)); // get/setField
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/objectToArray/ (23)
        assertExpression(
                Arrays.asList(Document.parse("{'k': 'k1', 'v': 1}")),
                MqlValues.<MqlInteger>ofMap().set("k1", of(1)).entrySet(),
                "{'$objectToArray': {'$setField': "
                        + "{'field': 'k1', 'input': {'$literal': {}}, 'value': 1}}}");

        // key/value usage
        assertExpression(
                "keyA|keyB|",
                mapA1B2.entrySet().map(v -> v.getKey().append(of("|"))).joinStrings(v -> v));
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

        // via getMap
        MqlDocument doc = of(Document.parse("{ instock: { warehouse1: 2500, warehouse2: 500 } }"));
        assertExpression(
                Arrays.asList(
                        Document.parse("{'k': 'warehouse1', 'v': 2500}"),
                        Document.parse("{'k': 'warehouse2', 'v': 500}")),
                doc.getMap("instock").entrySet(),
                "{'$objectToArray': {'$getField': {'input': {'$literal': "
                        + "{'instock': {'warehouse1': 2500, 'warehouse2': 500}}}, 'field': 'instock'}}}");
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

    @Test
    public void asDocumentTest() {
        MqlMap<MqlInteger> d = ofMap(BsonDocument.parse("{a: 1}"));
        assertSame(d, d.asDocument());
    }
}

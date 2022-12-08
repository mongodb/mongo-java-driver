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
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofMap;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("ConstantConditions")
class DocumentExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#object-expression-operators
    // (Complete as of 6.0)

    private static DocumentExpression ofDoc(final String ofDoc) {
        return of(BsonDocument.parse(ofDoc));
    }

    private final DocumentExpression a1 = ofDoc("{a: 1}");
    private final DocumentExpression ax1ay2 = ofDoc("{a: {x: 1, y: 2}}");

    @Test
    public void literalsTest() {
        assertExpression(
                BsonDocument.parse("{'a': 1}"),
                ofDoc("{a: 1}"),
                "{'$literal': {'a': 1}}");
        assertThrows(IllegalArgumentException.class, () -> of((Bson) null));
        // doc inside doc
        assertExpression(
                BsonDocument.parse("{'a': {'x': 1, 'y': 2}}"),
                ofDoc("{a: {x: 1, y: 2}}"),
                "{'$literal': {'a': {'x': 1, 'y': 2}}}");
        // empty
        assertExpression(
                BsonDocument.parse("{}"),
                ofDoc("{}"),
                "{'$literal': {}}");
        // ensure is literal
        assertExpression(BsonDocument.parse(
                "{'lit': {'$not': true}}"),
                of(BsonDocument.parse("{lit: {'$not': true} }")),
                "{'$literal': {'lit': {'$not': true}}}");
    }

    @Test
    public void getFieldTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/getField/ (100)
        // these count as assertions by the user that the value is of the correct type

        assertExpression(1,
                a1.getField("a"),
                "{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}");
        assertExpression(2,
                a1.getInteger("a").multiply(2),
                "{'$multiply': [{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}, 2]}");

        // different types
        String getFieldMql = "{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}";
        assertExpression(1, a1.getNumber("a"), getFieldMql);
        // these are all violations, since they assert the wrong type, but we are testing the generated Mql:
        assertExpression(1, a1.getBoolean("a"), getFieldMql);
        assertExpression(1, a1.getInteger("a"), getFieldMql);
        assertExpression(1, a1.getString("a"), getFieldMql);
        assertExpression(1, a1.getDate("a"), getFieldMql);
        assertExpression(1, a1.getArray("a"), getFieldMql);
        assertExpression(1, a1.getDocument("a"), getFieldMql);
        // usage with other expressions
        assertExpression(false, ofDoc("{a: true}").getBoolean("a").not());
        assertExpression(0.5, ofDoc("{a: 1.0}").getNumber("a").divide(2));
        assertExpression(8, ofIntegerArray(9, 8, 7).elementAt(ofDoc("{a: 1.0}").getInteger("a")));
        assertExpression("a", ofDoc("{a: 'A'}").getString("a").toLower());
        assertExpression(12, ofDoc("{a: {'$date': '2007-12-03T10:15:30.005Z'}}")
                .getDate("a").month(of("UTC")));
        assertExpression(3, ofDoc("{a: [3, 2]}").getArray("a").first());
        assertExpression(2, ofDoc("{a: {b: 2}}").getDocument("a").getInteger("b"));

        // field names, not paths
        DocumentExpression doc = ofDoc("{a: {b: 2}, 'a.b': 3, 'a$b': 4, '$a.b': 5}");
        assertExpression(2, doc.getDocument("a").getInteger("b"));
        assertExpression(3, doc.getInteger("a.b"));
        assertExpression(4, doc.getInteger("a$b"));
        assertExpression(5,
                doc.getInteger("$a.b"),
                "{'$getField': {'input': {'$literal': {'a': {'b': 2}, 'a.b': 3, 'a$b': 4, '$a.b': 5}}, "
                        + "'field': {'$literal': '$a.b'}}}");
    }

    @Test
    public void getFieldOrTest() {
        // convenience
        assertExpression(true, ofDoc("{a: true}").getBoolean("a", false));
        assertExpression(1.0, ofDoc("{a: 1.0}").getNumber("a", 99));
        assertExpression(1.0, ofDoc("{a: 1.0}").getNumber("a", Decimal128.parse("99")));
        assertExpression("A", ofDoc("{a: 'A'}").getString("a", "Z"));
        assertExpression(2007, ofDoc("{a: {'$date': '2007-12-03T10:15:30.005Z'}}")
                .getDate("a", Instant.EPOCH).year(of("UTC")));
        // no convenience for arrays
        assertExpression(Document.parse("{b: 2}"), ofDoc("{a: {b: 2}}")
                .getDocument("a", Document.parse("{z: 99}")));
        assertExpression(Document.parse("{b: 2}"), ofDoc("{a: {b: 2}}")
                .getMap("a", Document.parse("{z: 99}")));

        // normal
        assertExpression(true, ofDoc("{a: true}").getBoolean("a", of(false)));
        assertExpression(1.0, ofDoc("{a: 1.0}").getNumber("a", of(99)));
        assertExpression(1.0, ofDoc("{a: 1.0}").getInteger("a", of(99)));
        assertExpression("A", ofDoc("{a: 'A'}").getString("a", of("Z")));
        assertExpression(2007, ofDoc("{a: {'$date': '2007-12-03T10:15:30.005Z'}}")
                .getDate("a", of(Instant.EPOCH)).year(of("UTC")));
        assertExpression(Arrays.asList(3, 2), ofDoc("{a: [3, 2]}").getArray("a", ofIntegerArray(99, 88)));
        assertExpression(Document.parse("{b: 2}"), ofDoc("{a: {b: 2}}")
                .getDocument("a", of(Document.parse("{z: 99}"))));
        assertExpression(Document.parse("{b: 2}"), ofDoc("{a: {b: 2}}")
                .getMap("a", ofMap(Document.parse("{z: 99}"))));

        // right branch (missing field)
        assertExpression(false, ofDoc("{}").getBoolean("a", false));
        assertExpression(99, ofDoc("{}").getInteger("a", 99));
        assertExpression(99, ofDoc("{}").getNumber("a", 99));
        assertExpression(99L, ofDoc("{}").getNumber("a", 99L));
        assertExpression(99.0, ofDoc("{}").getNumber("a", 99.0));
        assertExpression(Decimal128.parse("99"), ofDoc("{}").getNumber("a", Decimal128.parse("99")));
        assertExpression("Z", ofDoc("{}").getString("a", "Z"));
        assertExpression(1970, ofDoc("{}")
                .getDate("a", Instant.EPOCH).year(of("UTC")));
        assertExpression(Arrays.asList(99, 88), ofDoc("{}").getArray("a", ofIntegerArray(99, 88)));
        assertExpression(Document.parse("{z: 99}"), ofDoc("{}")
                .getDocument("a", Document.parse("{z: 99}")));
        assertExpression(Document.parse("{z: 99}"), ofDoc("{}")
                .getMap("a", Document.parse("{z: 99}")));

        // int vs num
        assertExpression(99, ofDoc("{a: 1.1}").getInteger("a", of(99)));
    }

    @Test
    public void getFieldMissingTest() {
        // missing fields
        assertExpression(
                BsonDocument.parse("{'a': 1}"),
                a1.setField("z", a1.getBoolean("missing")));
        assertExpression(
                BsonDocument.parse("{'a': 1}"),
                a1.setField("z", a1.getDocument("missing").getDocument("also_missing")));
        assertExpression(
                BsonDocument.parse("{'a': 1, 'z': ''}"),
                a1.setField("z", a1.getString("missing").toLower()));
        /*
        The behaviour of missing fields appears to be as follows, and equivalent to $$REMOVE:
        propagates -- getField, cond branches...
        false -- not, or, cond check...
        0     -- sum...
        ""    -- toLower...
        null  -- multiply, add, subtract, year, filter, reduce, map, result within map...
        */
    }

    @Test
    public void setFieldTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setField/
        // Placing a field based on a literal:
        assertExpression(
                BsonDocument.parse("{a: 1, r: 2}"), // map.put("r", 2)
                a1.setField("r", of(2)),
                // MQL:
                "{'$setField': {'field': 'r', 'input': {'$literal': {'a': 1}}, 'value': 2}}");

        // Placing a null value:
        assertExpression(
                BsonDocument.parse("{a: 1, r: null}"), // map.put("r", null)
                a1.setField("r", Expressions.ofNull()),
                // MQL:
                "{'$setField': {'field': 'r', 'input': {'$literal': {'a': 1}}, 'value': null}}");

        // Replacing a field based on its prior value:
        assertExpression(
                BsonDocument.parse("{a: 3}"), // map.put("a", map.get("a") * 3)
                a1.setField("a", a1.getInteger("a").multiply(3)),
                // MQL:
                "{'$setField': {'field': 'a', 'input': {'$literal': {'a': 1}}, 'value': "
                        + "{'$multiply': [{'$getField': {'input': {'$literal': {'a': 1}}, 'field': 'a'}}, 3]}}}");

        // Placing a field based on a nested object:
        assertExpression(
                BsonDocument.parse("{'a': {'x': 1, 'y': 2}, r: 10}"),
                ax1ay2.setField("r", ax1ay2.getDocument("a").getInteger("x").multiply(10)),
                // MQL:
                "{'$setField': {'field': 'r', 'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, "
                        + "'value': {'$multiply': ["
                        + "  {'$getField': {'input': {'$getField': {'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, "
                        + "  'field': 'a'}}, 'field': 'x'}}, 10]}}}");

        // Replacing a nested object requires two setFields, as expected:
        assertExpression(
                // "with" syntax: [ { a:{x:1,y:2} } ].map(d -> d.with("a", d.a.with("y", d.a.y.multiply(11))))
                BsonDocument.parse("{'a': {'x': 1, 'y': 22}}"),
                ax1ay2.setField("a", ax1ay2.getDocument("a")
                        .setField("y", ax1ay2.getDocument("a").getInteger("y").multiply(11))),
                "{'$setField': {'field': 'a', 'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, "
                        + "'value': {'$setField': {'field': 'y', 'input': {'$getField': "
                        + "{'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, 'field': 'a'}}, "
                        + "'value': {'$multiply': [{'$getField': {'input': {'$getField': "
                        + "{'input': {'$literal': {'a': {'x': 1, 'y': 2}}}, 'field': 'a'}}, "
                        + "'field': 'y'}}, 11]}}}}}");
    }

    @Test
    public void unsetFieldTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/unsetField/
        assertExpression(
                BsonDocument.parse("{}"), // map.remove("a")
                a1.unsetField("a"),
                // MQL:
                "{'$unsetField': {'field': 'a', 'input': {'$literal': {'a': 1}}}}");
    }

    @Test
    public void mergeTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/mergeObjects/
        assertExpression(
                BsonDocument.parse("{a: 1, b: 2}"),
                ofDoc("{a: 1}").merge(ofDoc("{b: 2}")),
                "{'$mergeObjects': [{'$literal': {'a': 1}}, {'$literal': {'b': 2}}]}");

        assertExpression(
                BsonDocument.parse("{a: null}"),
                ofDoc("{a: 1}").merge(ofDoc("{a: null}")));

        assertExpression(
                BsonDocument.parse("{a: 1}"),
                ofDoc("{a: null}").merge(ofDoc("{a: 1}")));
    }
}

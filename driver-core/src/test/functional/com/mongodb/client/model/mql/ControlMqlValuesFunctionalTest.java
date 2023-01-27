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

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.function.Function;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.mql.MqlValues.of;
import static com.mongodb.client.model.mql.MqlValues.ofArray;
import static com.mongodb.client.model.mql.MqlValues.ofIntegerArray;
import static com.mongodb.client.model.mql.MqlValues.ofMap;
import static com.mongodb.client.model.mql.MqlValues.ofNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ControlMqlValuesFunctionalTest extends AbstractMqlValuesFunctionalTest {

    @Test
    public void passToTest() {
        Function<MqlInteger, MqlInteger> intDecrement = (e) -> e.subtract(of(1));
        Function<MqlNumber, MqlNumber> numDecrement = (e) -> e.subtract(of(1));

        // "nested functional" function application:
        assertExpression(
                2 - 1,
                intDecrement.apply(of(2)),
                "{'$subtract': [2, 1]}");
        // "chained" function application produces the same MQL:
        assertExpression(
                2 - 1,
                of(2).passIntegerTo(intDecrement),
                "{'$subtract': [2, 1]}");

        // variations
        assertExpression(
                2 - 1,
                of(2).passIntegerTo(numDecrement));
        assertExpression(
                2 - 1,
                of(2).passNumberTo(numDecrement));

        // all types
        Function<MqlValue, MqlString> test = on -> of("A");
        assertExpression("A", of(true).passTo(test));
        assertExpression("A", of(false).passBooleanTo(test));
        assertExpression("A", of(0).passIntegerTo(test));
        assertExpression("A", of(0).passNumberTo(test));
        assertExpression("A", of("").passStringTo(test));
        assertExpression("A", of(Instant.ofEpochMilli(123)).passDateTo(test));
        assertExpression("A", ofIntegerArray(1, 2).passArrayTo(test));
        assertExpression("A", of(Document.parse("{_id: 'a'}")).passDocumentTo(test));
        assertExpression("A", ofMap(Document.parse("{_id: 'a'}")).passMapTo(test));
    }

    @Test
    public void switchTest() {
        assumeTrue(serverVersionAtLeast(4, 4)); // isNumber
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/switch/
        assertExpression("a", of(0).switchOn(on -> on.is(v -> v.eq(of(0)), v -> of("a"))));
        assertExpression("a", of(0).switchOn(on -> on.isNumber(v -> of("a"))));
        assertExpression("a", of(0).switchOn(on -> on.eq(of(0), v -> of("a"))));
        assertExpression("a", of(0).switchOn(on -> on.lte(of(9), v -> of("a"))));

        // test branches
        Function<MqlInteger, MqlBoolean> isOver10 = v -> v.subtract(10).gt(of(0));
        Function<MqlInteger, MqlString> s = e -> e
                .switchIntegerOn(on -> on
                        .eq(of(0), v -> of("A"))
                        .lt(of(10), v -> of("B"))
                        .is(isOver10, v -> of("C"))
                        .defaults(v -> of("D")))
                .toLower();

        assertExpression("a", of(0).passIntegerTo(s));
        assertExpression("b", of(9).passIntegerTo(s));
        assertExpression("b", of(-9).passIntegerTo(s));
        assertExpression("c", of(11).passIntegerTo(s));
        assertExpression("d", of(10).passIntegerTo(s));
    }

    @Test
    public void switchInferenceTest() {
        // the following must compile:
        assertExpression(
                "b",
                of(1).switchOn(on -> on
                        .eq(of(0), v -> of("a"))
                        .eq(of(1), v -> of("b"))
                ));
        // the "of(0)" must not cause a type inference of T being an integer,
        // since switchOn expects an Expression.
    }

    @Test
    public void switchTypesTest() {
        // isIntegerOr relies on switch short-circuiting, which only happens after 5.2
        assumeTrue(serverVersionAtLeast(5, 2));
        Function<MqlValue, MqlString> label = expr -> expr.switchOn(on -> on
                .isBoolean(v -> v.asString().append(of(" - bool")))
                // integer should be checked before string
                .isInteger(v -> v.asString().append(of(" - integer")))
                .isNumber(v -> v.asString().append(of(" - number")))
                .isString(v -> v.asString().append(of(" - string")))
                .isDate(v -> v.asString().append(of(" - date")))
                .isArray((MqlArray<MqlInteger> v) -> v.sum(a -> a).asString().append(of(" - array")))
                .isDocument(v -> v.getString("_id").append(of(" - document")))
                .isNull(v -> of("null - null"))
                .defaults(v -> of("default"))
                ).toLower();
        assertExpression("true - bool", of(true).passTo(label));
        assertExpression("false - bool", of(false).passBooleanTo(label));
        assertExpression("1 - integer", of(1).passIntegerTo(label));
        assertExpression("1 - integer", of(1.0).passNumberTo(label));
        assertExpression("1.01 - number", of(1.01).passNumberTo(label));
        assertExpression("abc - string", of("abc").passStringTo(label));
        assertExpression("1970-01-01t00:00:00.123z - date", of(Instant.ofEpochMilli(123)).passDateTo(label));
        assertExpression("3 - array", ofIntegerArray(1, 2).passArrayTo(label));
        assertExpression("a - document", of(Document.parse("{_id: 'a'}")).passDocumentTo(label));
        // maps are considered documents
        assertExpression("a - document", ofMap(Document.parse("{_id: 'a'}")).passMapTo(label));
        assertExpression("null - null", ofNull().passTo(label));
        // maps via isMap:
        assertExpression(
                "12 - map",
                ofMap(Document.parse("{a: '1', b: '2'}")).switchOn(on -> on
                        .isMap((MqlMap<MqlString> v) -> v.entrySet()
                                .joinStrings(e -> e.getValue()).append(of(" - map")))));
        // arrays via isArray, and tests signature:
        assertExpression(
                "ab - array",
                ofArray(of("a"), of("b")).switchOn(on -> on
                        .isArray((MqlArray<MqlString> v) -> v
                                .joinStrings(e -> e).append(of(" - array")))));
    }

    private <T extends MqlValue> BranchesIntermediary<T, MqlString> branches(final Branches<T> on) {
        return on.is(v -> of(true), v -> of("A"));
    }

    @Test
    public void switchTestVariants() {
        assertExpression("A", of(true).switchOn(this::branches));
        assertExpression("A", of(false).switchBooleanOn(this::branches));
        assertExpression("A", of(0).switchIntegerOn(this::branches));
        assertExpression("A", of(0).switchNumberOn(this::branches));
        assertExpression("A", of("").switchStringOn(this::branches));
        assertExpression("A", of(Instant.ofEpochMilli(123)).switchDateOn(this::branches));
        assertExpression("A", ofIntegerArray(1, 2).switchArrayOn(this::branches));
        assertExpression("A", of(Document.parse("{_id: 'a'}")).switchDocumentOn(this::branches));
        assertExpression("A", ofMap(Document.parse("{_id: 'a'}")).switchMapOn(this::branches));
    }

    @Test
    public void switchTestInitial() {
        assertExpression("A",
                of(0).switchOn(on -> on.is(v -> v.gt(of(-1)), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$gt': [0, -1]}, 'then': 'A'}]}}");
        // eq lt lte
        assertExpression("A",
                of(0).switchOn(on -> on.eq(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, 0]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchOn(on -> on.lt(of(1), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$lt': [0, 1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchOn(on -> on.lte(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$lte': [0, 0]}, 'then': 'A'}]}}");
        // is type
        assertExpression("A",
                of(true).switchOn(on -> on.isBoolean(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': [true]}, 'bool']}, 'then': 'A'}]}}");
        assertExpression("A",
                of("x").switchOn(on -> on.isString(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': ['x']}, 'string']}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Instant.ofEpochMilli(123)).switchOn(on -> on.isDate(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$in': [{'$type': "
                        + "[{'$date': '1970-01-01T00:00:00.123Z'}]}, ['date']]}, 'then': 'A'}]}}");
        assertExpression("A",
                ofIntegerArray(0).switchOn(on -> on.isArray(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$isArray': [[0]]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Document.parse("{}")).switchOn(on -> on.isDocument(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': "
                        + "[{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofMap(Document.parse("{}")).switchOn(on -> on.isMap(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': "
                        + "[{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofNull().switchOn(on -> on.isNull(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [null, null]}, 'then': 'A'}]}}");
    }

    @Test
    public void switchTestInitialVersion44() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assertExpression("A",
                of(1).switchOn(on -> on.isNumber(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$isNumber': [1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(1).switchOn(on -> on.isInteger(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$switch': {'branches': [{'case': {'$isNumber': [1]},"
                        + "'then': {'$eq': [{'$round': 1}, 1]}}], 'default': false}}, 'then': 'A'}]}}");
    }
    @Test
    public void switchTestPartialVersion44() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assertExpression("A",
                of(1).switchOn(on -> on.isNull(v -> of("X")).isNumber(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [1, null]}, 'then': 'X'}, "
                        + "{'case': {'$isNumber': [1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(1).switchOn(on -> on.isNull(v -> of("X")).isInteger(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [1, null]}, 'then': 'X'}, {'case': "
                        + "{'$switch': {'branches': [{'case': {'$isNumber': [1]}, "
                        + "'then': {'$eq': [{'$round': 1}, 1]}}], 'default': false}}, 'then': 'A'}]}}");
        assertExpression("A",
                ofNull().switchOn(on -> on.isNumber(v -> of("X")).isNull(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$isNumber': [null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [null, null]}, 'then': 'A'}]}}");
    }

    @Test
    public void switchTestPartial() {
        assertExpression("A",
                of(0).switchOn(on -> on.isNull(v -> of("X")).is(v -> v.gt(of(-1)), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$gt': [0, -1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchOn(on -> on.isNull(v -> of("X")).defaults(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}], "
                        + "'default': 'A'}}");
        // eq lt lte
        assertExpression("A",
                of(0).switchOn(on -> on.isNull(v -> of("X")).eq(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [0, 0]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchOn(on -> on.isNull(v -> of("X")).lt(of(1), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$lt': [0, 1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchOn(on -> on.isNull(v -> of("X")).lte(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$lte': [0, 0]}, 'then': 'A'}]}}");
        // is type
        assertExpression("A",
                of(true).switchOn(on -> on.isNull(v -> of("X")).isBoolean(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [true, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': [true]}, 'bool']}, 'then': 'A'}]}}");
        assertExpression("A",
                of("x").switchOn(on -> on.isNull(v -> of("X")).isString(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': ['x', null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': ['x']}, 'string']}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Instant.ofEpochMilli(123)).switchOn(on -> on.isNull(v -> of("X")).isDate(v -> of("A"))),
                "{'$switch': {'branches': ["
                        + "{'case': {'$eq': [{'$date': '1970-01-01T00:00:00.123Z'}, null]}, 'then': 'X'}, "
                        + "{'case': {'$in': [{'$type': [{'$date': '1970-01-01T00:00:00.123Z'}]}, "
                        + "['date']]}, 'then': 'A'}]}}");
        assertExpression("A",
                ofIntegerArray(0).switchOn(on -> on.isNull(v -> of("X")).isArray(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [[0], null]}, 'then': 'X'}, "
                        + "{'case': {'$isArray': [[0]]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Document.parse("{}")).switchOn(on -> on.isNull(v -> of("X")).isDocument(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$literal': {}}, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': [{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofMap(Document.parse("{}")).switchOn(on -> on.isNull(v -> of("X")).isMap(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$literal': {}}, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': [{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
    }
}

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

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofMap;
import static com.mongodb.client.model.expressions.Expressions.ofNull;

class ControlExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {

    @Test
    public void applyTest() {
        Function<IntegerExpression, IntegerExpression> decrement = (e) -> e.subtract(of(1));
        // "nested functional" function application:
        assertExpression(
                2 - 1,
                decrement.apply(of(2)),
                "{'$subtract': [2, 1]}");
        // "chained" function application:
        assertExpression(
                2 - 1, // = 0
                of(2).apply(decrement),
                "{'$subtract': [2, 1]}");
        // the parameters are reversed, compared to Java's function.apply
    }

    @Test
    public void switchTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/switch/
        assertExpression("a", of(0).switchMap(on -> on.is(v -> v.eq(of(0)), v -> of("a"))));
        assertExpression("a", of(0).switchMap(on -> on.isNumber(v -> of("a"))));
        assertExpression("a", of(0).switchMap(on -> on.lte(of(9), v -> of("a"))));

        // test branches
        Function<IntegerExpression, BooleanExpression> isOver10 = v -> v.subtract(10).gt(of(0));
        Function<IntegerExpression, StringExpression> s = e -> e
                .switchMap((Branches on) -> on
                        .eq(of(0), v -> of("A"))
                        .lt(of(10), v -> of("B"))
                        .is(isOver10, v -> of("C"))
                        .defaults(v -> of("D")))
                .toLower();

        assertExpression("a", of(0).apply(s));
        assertExpression("b", of(9).apply(s));
        assertExpression("b", of(-9).apply(s));
        assertExpression("c", of(11).apply(s));
        assertExpression("d", of(10).apply(s));
    }

    @Test
    public void switchTypesTest() {
        Function<IntegerExpression, StringExpression> label = expr -> expr.switchMap(on -> on
                .isBoolean(v -> v.asString().concat(of(" - bool")))
                .isNumber(v -> v.asString().concat(of(" - number")))
                .isString(v -> v.asString().concat(of(" - string")))
                .isDate(v -> v.asString().concat(of(" - date")))
                .isArray((ArrayExpression<IntegerExpression> v) -> v.sum(a -> a).asString().concat(of(" - array")))
                .isDocument(v -> v.getString("_id").concat(of(" - document")))
                .isNull(v -> of("null - null"))
                .defaults(v -> of("default"))
                ).toLower();
        assertExpression("true - bool", of(true).apply(label));
        assertExpression("false - bool", of(false).apply(label));
        assertExpression("1 - number", of(1).apply(label));
        assertExpression("1 - number", of(1.0).apply(label));
        assertExpression("abc - string", of("abc").apply(label));
        assertExpression("1970-01-01t00:00:00.123z - date", of(Instant.ofEpochMilli(123)).apply(label));
        assertExpression("3 - array", ofIntegerArray(1, 2).apply(label));
        assertExpression("a - document", of(Document.parse("{_id: 'a'}")).apply(label));
        assertExpression("null - null", ofNull().apply(label));
        assertExpression(
                "ab - map",
                ofMap(Document.parse("{a: 1, b: 2}")).switchMap(on -> on
                        .isMap((MapExpression<StringExpression> v) -> v.entrySet()
                                .join(e -> e.getKey()).concat(of(" - map")))));
    }

    @Test
    public void switchTestInitial() {
        assertExpression("A",
                of(0).switchMap(on -> on.is(v -> v.gt(of(-1)), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$gt': [0, -1]}, 'then': 'A'}]}}");
        // eq lt lte
        assertExpression("A",
                of(0).switchMap(on -> on.eq(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, 0]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchMap(on -> on.lt(of(1), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$lt': [0, 1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchMap(on -> on.lte(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$lte': [0, 0]}, 'then': 'A'}]}}");
        // is type
        assertExpression("A",
                of(true).switchMap(on -> on.isBoolean(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': [true]}, 'bool']}, 'then': 'A'}]}}");
        assertExpression("A",
                of(1).switchMap(on -> on.isNumber(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$isNumber': [1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of("x").switchMap(on -> on.isString(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': ['x']}, 'string']}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Instant.ofEpochMilli(123)).switchMap(on -> on.isDate(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$in': [{'$type': "
                        + "[{'$date': '1970-01-01T00:00:00.123Z'}]}, ['date', 'timestamp']]}, 'then': 'A'}]}}");
        assertExpression("A",
                ofIntegerArray(0).switchMap(on -> on.isArray(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$isArray': [[0]]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Document.parse("{}")).switchMap(on -> on.isDocument(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': "
                        + "[{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofMap(Document.parse("{}")).switchMap(on -> on.isMap(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$type': "
                        + "[{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofNull().switchMap(on -> on.isNull(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [null, null]}, 'then': 'A'}]}}");
    }

    @Test
    public void switchTestPartial() {
        assertExpression("A",
                of(0).switchMap(on -> on.isNull(v -> of("X")).is(v -> v.gt(of(-1)), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$gt': [0, -1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchMap(on -> on.isNull(v -> of("X")).defaults(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}], "
                        + "'default': 'A'}}");
        // eq lt lte
        assertExpression("A",
                of(0).switchMap(on -> on.isNull(v -> of("X")).eq(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [0, 0]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchMap(on -> on.isNull(v -> of("X")).lt(of(1), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$lt': [0, 1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(0).switchMap(on -> on.isNull(v -> of("X")).lte(of(0), v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [0, null]}, 'then': 'X'}, "
                        + "{'case': {'$lte': [0, 0]}, 'then': 'A'}]}}");
        // is type
        assertExpression("A",
                of(true).switchMap(on -> on.isNull(v -> of("X")).isBoolean(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [true, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': [true]}, 'bool']}, 'then': 'A'}]}}");
        assertExpression("A",
                of(1).switchMap(on -> on.isNull(v -> of("X")).isNumber(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [1, null]}, 'then': 'X'}, "
                        + "{'case': {'$isNumber': [1]}, 'then': 'A'}]}}");
        assertExpression("A",
                of("x").switchMap(on -> on.isNull(v -> of("X")).isString(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': ['x', null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': ['x']}, 'string']}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Instant.ofEpochMilli(123)).switchMap(on -> on.isNull(v -> of("X")).isDate(v -> of("A"))),
                "{'$switch': {'branches': ["
                        + "{'case': {'$eq': [{'$date': '1970-01-01T00:00:00.123Z'}, null]}, 'then': 'X'}, "
                        + "{'case': {'$in': [{'$type': [{'$date': '1970-01-01T00:00:00.123Z'}]}, "
                        + "['date', 'timestamp']]}, 'then': 'A'}]}}");
        assertExpression("A",
                ofIntegerArray(0).switchMap(on -> on.isNull(v -> of("X")).isArray(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [[0], null]}, 'then': 'X'}, "
                        + "{'case': {'$isArray': [[0]]}, 'then': 'A'}]}}");
        assertExpression("A",
                of(Document.parse("{}")).switchMap(on -> on.isNull(v -> of("X")).isDocument(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$eq': [{'$literal': {}}, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': [{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofMap(Document.parse("{}")).switchMap(on -> on.isNull(v -> of("X")).isMap(v -> of("A"))),
                " {'$switch': {'branches': ["
                        + "{'case': {'$eq': [{'$literal': {}}, null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [{'$type': [{'$literal': {}}]}, 'object']}, 'then': 'A'}]}}");
        assertExpression("A",
                ofNull().switchMap(on -> on.isNumber(v -> of("X")).isNull(v -> of("A"))),
                "{'$switch': {'branches': [{'case': {'$isNumber': [null]}, 'then': 'X'}, "
                        + "{'case': {'$eq': [null, null]}, 'then': 'A'}]}}");
    }
}

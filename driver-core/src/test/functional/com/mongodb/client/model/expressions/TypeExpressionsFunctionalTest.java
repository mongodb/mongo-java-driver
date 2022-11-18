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

import com.mongodb.MongoCommandException;
import org.bson.BsonDocument;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TypeExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#type-expression-operators

    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/ (28 |40)
    // type is not implemented directly; instead, similar checks done via switch

    // the direct "isT" (comparable to instanceof) are exposed via switch
    // here, we expose isTypeOr. These would be used on an Expression of
    // an unknown type, or to provide default values in cases where a null
    // has intruded into the alleged type.

    @Test
    public void isBooleanOrTest() {
        assertExpression(
                true,
                of(true).isBooleanOr(of(false)),
                "{'$cond': [{'$eq': [{'$type': true}, 'bool']}, true, false]}");
        // non-boolean:
        assertExpression(false, ofIntegerArray(1).isBooleanOr(of(false)));
        assertExpression(false, ofNull().isBooleanOr(of(false)));
    }

    @Test
    public void isNumberOrTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isNumber/ (99 |87)
        assertExpression(1, of(1).isNumberOr(of(99)), "{'$cond': [{'$isNumber': [1]}, 1, 99]}");
        // other numeric values:
        assertExpression(1L, of(1L).isNumberOr(of(99)));
        assertExpression(1.0, of(1.0).isNumberOr(of(99)));
        assertExpression(Decimal128.parse("1"), of(Decimal128.parse("1")).isNumberOr(of(99)));
        // non-numeric:
        assertExpression(99, ofIntegerArray(1).isNumberOr(of(99)));
        assertExpression(99, ofNull().isNumberOr(of(99)));
    }

    @Test
    public void isStringOrTest() {
        assertExpression(
                "abc",
                of("abc").isStringOr(of("or")),
                "{'$cond': [{'$eq': [{'$type': 'abc'}, 'string']}, 'abc', 'or']}");
        // non-string:
        assertExpression("or", ofIntegerArray(1).isStringOr(of("or")));
        assertExpression("or", ofNull().isStringOr(of("or")));
    }

    @Test
    public void isDateOrTest() {
        Instant date = Instant.parse("2007-12-03T10:15:30.005Z");
        assertExpression(
                Instant.ofEpochMilli(1196676930005L),
                of(date).isDateOr(of(date.plusMillis(10))),
                "{'$cond': [{'$in': [{'$type': {'$date': '2007-12-03T10:15:30.005Z'}}, "
                        + "['date', 'timestamp']]}, {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "{'$date': '2007-12-03T10:15:30.015Z'}]}");
        // non-date:
        assertExpression(date, ofIntegerArray(1).isDateOr(of(date)));
        assertExpression(date, ofNull().isDateOr(of(date)));
    }

    @Test
    public void isArrayOrTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isArray/ (36 |47)
        assertExpression(
                Arrays.asList(1, 2),
                ofIntegerArray(1, 2).isArrayOr(ofIntegerArray(99)),
                "{'$cond': [{'$isArray': [[1, 2]]}, [1, 2], [99]]}");
        // non-array:
        assertExpression(Arrays.asList(1, 2), of(true).isArrayOr(ofIntegerArray(1, 2)));
        assertExpression(Arrays.asList(1, 2), ofNull().isArrayOr(ofIntegerArray(1, 2)));
    }

    @Test
    public void isDocumentOrTest() {
        BsonDocument doc = BsonDocument.parse("{a: 1}");
        assertExpression(doc,
                of(doc).isDocumentOr(of(BsonDocument.parse("{b: 2}"))),
                "{'$cond': [{'$eq': [{'$type': {'$literal': {'a': 1}}}, 'object']}, "
                        + "{'$literal': {'a': 1}}, {'$literal': {'b': 2}}]}");
        // non-document:
        assertExpression(doc, ofIntegerArray(1).isDocumentOr(of(doc)));
        assertExpression(doc, ofNull().isDocumentOr(of(doc)));
    }

    // conversions
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/ (38 |40)
    // Convert is not implemented: too dynamic, conversions should be explicit.

    /*
    Useful conversions:
    - anything-string: toString to a parsable type
    - string-parse-anything: every type should allow parsing to a string
      - presently excludes objects (docs) and arrays
      - includes formatted date strings
    - milliseconds since epoch to date

    Convert also defines many conversions that do not seem useful:
    - boolean-number conversions: t/f to-from 1/0
    - boolean-other (oid/str/date) - always true, broken for strings; pointless
    - number-number - "underlying" json type is changed, possible exceptions?
     */

    @Test
    public void asStringTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toString/
        // asString, since toString conflicts
        assertExpression("false", of(false).asString(), "{'$toString': [false]}");

        assertExpression("1", of(1).asString());
        assertExpression("1", of(1L).asString());
        assertExpression("1", of(1.0).asString());
        assertExpression("1.0", of(Decimal128.parse("1.0")).asString());

        assertExpression("abc", of("abc").asString());

        // this is equivalent to $dateToString
        assertExpression("1970-01-01T00:00:00.123Z", of(Instant.ofEpochMilli(123)).asString());

        // TODO:
//        assertExpression("[]", ofIntegerArray(1, 2).asString());
//        assertExpression("[1, 2]", ofIntegerArray(1, 2).asString());
//        assertExpression("{a: 1}", of(Document.parse("{a: 1}")).asString());
    }

    @Test
    public void dateAsStringTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        final Instant instant = Instant.parse("2007-12-03T10:15:30.005Z");
        assertExpression(
                "2007-12-03T10:15:30.005Z",
                of(instant).asString(),
                "{'$toString': [{'$date': '2007-12-03T10:15:30.005Z'}]}");

        // with parameters
        assertExpression(
                "2007-12-03T05:15:30.005Z",
                of(instant).asString(of("%Y-%m-%dT%H:%M:%S.%LZ"), of("America/New_York")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%LZ', "
                        + "'timezone': 'America/New_York'}}");
        assertExpression(
                "2007-12-03T14:45:30.005Z",
                of(instant).asString(of("%Y-%m-%dT%H:%M:%S.%LZ"), of("+04:30")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%LZ', "
                        + "'timezone': '+04:30'}}");
    }

    // parse string

    @Test
    public void parseDateTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/ (45 |69)
        String stringDate = "2007-12-03T10:15:30.005Z";
        final Instant instant = Instant.parse(stringDate);
        assertExpression(
                instant,
                of(stringDate).parseDate(),
                "{'$dateFromString': {'dateString': '2007-12-03T10:15:30.005Z'}}");

        // with parameters
        assertExpression(
                instant,
                of("2007-12-03T05:15:30.005Z").parseDate(of("%Y-%m-%dT%H:%M:%S.%LZ"), of("America/New_York")),
                "{'$dateFromString': {'dateString': '2007-12-03T05:15:30.005Z', "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%LZ', 'timezone': 'America/New_York'}}");
        assertExpression(
                instant,
                of("2007-12-03T14:45:30.005Z").parseDate(of("%Y-%m-%dT%H:%M:%S.%LZ"), of("+04:30")),
                "{'$dateFromString': {'dateString': '2007-12-03T14:45:30.005Z', "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%LZ', 'timezone': '+04:30'}}");
    }

    @Test
    public void parseIntegerTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toInt/ (46 |15)
        assertExpression(1234L, of("1234").parseInteger(), "{'$toLong': '1234'}");
        // TODO: note that this parses to long. Unclear how to dynamically choose int/long
    }

    @Test
    public void parseObjectIdTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toObjectId/ (39 |28)
        // TODO objectId has no type of its own, but this might be fine
        assertExpression(
                new ObjectId("5ab9cbfa31c2ab715d42129e"),
                of("5ab9cbfa31c2ab715d42129e").parseObjectId(),
                "{'$toObjectId': '5ab9cbfa31c2ab715d42129e'}");
    }

    // non-string

    @Test
    public void msToDateTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDate/ (36 |53)
        assertExpression(
                Instant.ofEpochMilli(1234),
                of(1234L).msToDate(),
                "{'$toDate': {'$numberLong': '1234'}}");
        // could be: millisecondsToDate / epochMsToDate
        // TODO does not accept plain integers; could convert to dec128?
        assertThrows(MongoCommandException.class, () ->
                assertExpression(
                        Instant.parse("2007-12-03T10:15:30.005Z"),
                        of(1234).msToDate(),
                        "{'$toDate': 1234}"));
    }
}

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
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofNull;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TypeExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#type-expression-operators

    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/
    // type is not implemented directly; instead, similar checks done via switch

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
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isNumber/
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
                date,
                of(date).isDateOr(of(date.plusMillis(10))),
                "{'$cond': [{'$in': [{'$type': {'$date': '2007-12-03T10:15:30.005Z'}}, ['date']]}, "
                        + "{'$date': '2007-12-03T10:15:30.005Z'}, {'$date': '2007-12-03T10:15:30.015Z'}]}");
        // non-date:
        assertExpression(date, ofIntegerArray(1).isDateOr(of(date)));
        assertExpression(date, ofNull().isDateOr(of(date)));
    }

    @Test
    public void isArrayOrTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isArray/
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
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
    // Convert is not implemented: too dynamic, conversions should be explicit.

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

        // Arrays and documents are not (yet) supported:
        assertThrows(MongoCommandException.class, () ->
                assertExpression("[]", ofIntegerArray(1, 2).asString()));
        assertThrows(MongoCommandException.class, () ->
                assertExpression("[1, 2]", ofIntegerArray(1, 2).asString()));
        assertThrows(MongoCommandException.class, () ->
                assertExpression("{a: 1}", of(Document.parse("{a: 1}")).asString()));
    }

    @Test
    public void dateAsStringTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        final Instant instant = Instant.parse("2007-12-03T10:15:30.005Z");
        DateExpression date = of(instant);
        ZonedDateTime utcDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of(ZoneOffset.UTC.getId()));
        assertExpression(
                "2007-12-03T10:15:30.005Z",
                of(instant).asString(),
                "{'$toString': [{'$date': '2007-12-03T10:15:30.005Z'}]}");
        // with parameters
        assertExpression(
                utcDateTime.withZoneSameInstant(ZoneId.of("America/New_York")).format(ISO_LOCAL_DATE_TIME),
                date.asString(of("America/New_York"), of("%Y-%m-%dT%H:%M:%S.%L")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L', "
                        + "'timezone': 'America/New_York'}}");
        assertExpression(
                utcDateTime.withZoneSameInstant(ZoneId.of("+04:30")).format(ISO_LOCAL_DATE_TIME),
                date.asString(of("+04:30"), of("%Y-%m-%dT%H:%M:%S.%L")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L', "
                        + "'timezone': '+04:30'}}");
        // Olson Timezone Identifier is changed to UTC offset:
        assertExpression(
                "2007-12-03T05:15:30.005-0500",
                of(instant).asString(of("America/New_York"), of("%Y-%m-%dT%H:%M:%S.%L%z")));
    }

    // parse string

    @Test
    public void parseDateTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        String dateString = "2007-12-03T10:15:30.005Z";
        assertExpression(
                Instant.parse(dateString),
                of(dateString).parseDate(),
                "{'$dateFromString': {'dateString': '2007-12-03T10:15:30.005Z'}}");


        // throws: "cannot pass in a date/time string with GMT offset together with a timezone argument"
        assertThrows(MongoCommandException.class, () ->
                assertExpression(1, of("2007-12-03T10:15:30.005+01:00")
                        .parseDate(of("+01:00"), of("%Y-%m-%dT%H:%M:%S.%L%z"))
                        .asString()));
        // therefore, to parse date strings containing UTC offsets, we need:
        assertExpression(
                Instant.parse("2007-12-03T09:15:30.005Z"),
                of("2007-12-03T10:15:30.005+01:00")
                    .parseDate(of("%Y-%m-%dT%H:%M:%S.%L%z")),
                "{'$dateFromString': {'dateString': '2007-12-03T10:15:30.005+01:00', "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L%z'}}");
    }

    @Test
    public void parseIntegerTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toInt/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLong/
        assertExpression(
                1234,
                of("1234").parseInteger(),
                "{'$convert': {'input': '1234', 'onError': {'$toLong': '1234'}, 'to': 'int'}}");

        int intVal = 2_000_000_000;
        long longVal = 4_000_000_000L;
        assertExpression(
                intVal,
                of(intVal + "").parseInteger());
        assertExpression(
                longVal,
                of(longVal + "").parseInteger());
    }

    // non-string

    @Test
    public void millisecondsToDateTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDate/
        assertExpression(
                Instant.ofEpochMilli(1234),
                of(1234L).millisecondsToDate(),
                "{'$toDate': {'$numberLong': '1234'}}");
        // This does not accept plain integers:
        assertThrows(MongoCommandException.class, () ->
                assertExpression(
                        Instant.parse("2007-12-03T10:15:30.005Z"),
                        of(1234).millisecondsToDate(),
                        "{'$toDate': 1234}"));
    }
}

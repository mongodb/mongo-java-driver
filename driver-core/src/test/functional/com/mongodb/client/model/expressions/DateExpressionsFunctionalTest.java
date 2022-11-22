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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static com.mongodb.client.model.expressions.Expressions.of;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

class DateExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#date-expression-operators

    private final ZonedDateTime utcDateTime = ZonedDateTime.ofInstant(Instant.parse("2007-12-03T10:15:30.005Z"), ZoneId.of(ZoneOffset.UTC.getId()));
    private final StringExpression utc = of("UTC");
    private final DateExpression utcDateEx = of(utcDateTime.toInstant());

    @Test
    public void literalsTest() {
        assertExpression(
                utcDateTime.toInstant(), utcDateEx,
                "{'$date': '2007-12-03T10:15:30.005Z'}");
    }

    @Test
    public void yearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/
        assertExpression(
                utcDateTime.get(ChronoField.YEAR),
                utcDateEx.year(utc),
                "{'$year': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void monthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/
        assertExpression(
                utcDateTime.get(ChronoField.MONTH_OF_YEAR),
                utcDateEx.month(utc),
                "{'$month': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dayOfMonthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_MONTH),
                utcDateEx.dayOfMonth(utc),
                "{'$dayOfMonth': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dayOfWeekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfWeek/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_WEEK) + 1,
                utcDateEx.dayOfWeek(utc),
                "{'$dayOfWeek': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dayOfYearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfYear/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_YEAR),
                utcDateEx.dayOfYear(utc),
                "{'$dayOfYear': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void hourTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/
        assertExpression(
                utcDateTime.get(ChronoField.HOUR_OF_DAY),
                utcDateEx.hour(utc),
                "{'$hour': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void minuteTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/minute/
        assertExpression(
                utcDateTime.get(ChronoField.MINUTE_OF_HOUR),
                utcDateEx.minute(utc),
                "{'$minute': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void secondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/second/
        assertExpression(
                utcDateTime.get(ChronoField.SECOND_OF_MINUTE),
                utcDateEx.second(utc),
                "{'$second': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void weekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/
        assertExpression(
                48,
                utcDateEx.week(utc),
                "{'$week': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void millisecondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/millisecond/
        assertExpression(
                utcDateTime.get(ChronoField.MILLI_OF_SECOND),
                utcDateEx.millisecond(utc),
                "{'$millisecond': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dateToStringTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        assertExpression(
                utcDateTime.toInstant().toString(),
                utcDateEx.dateToString(),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}}}");
        // with parameters
        assertExpression(
                utcDateTime.withZoneSameInstant(ZoneId.of("America/New_York")).format(ISO_LOCAL_DATE_TIME),
                utcDateEx.dateToString(of("America/New_York"), of("%Y-%m-%dT%H:%M:%S.%L")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L', "
                        + "'timezone': 'America/New_York'}}");
        assertExpression(
                utcDateTime.withZoneSameInstant(ZoneId.of("+04:30")).format(ISO_LOCAL_DATE_TIME),
                utcDateEx.dateToString(of("+04:30"), of("%Y-%m-%dT%H:%M:%S.%L")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L', "
                        + "'timezone': '+04:30'}}");
    }
}

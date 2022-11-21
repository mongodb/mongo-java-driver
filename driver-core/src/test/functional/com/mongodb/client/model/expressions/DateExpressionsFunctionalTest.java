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

    @Test
    public void literalsTest() {
        assertExpression(
                utcDateTime.toInstant(),
                of(utcDateTime.toInstant()),
                "{'$date': '2007-12-03T10:15:30.005Z'}");
    }

    @Test
    public void yearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/
        assertExpression(
                utcDateTime.get(ChronoField.YEAR),
                of(utcDateTime.toInstant()).year(),
                "{'$year': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void monthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/
        assertExpression(
                utcDateTime.get(ChronoField.MONTH_OF_YEAR),
                of(utcDateTime.toInstant()).month(),
                "{'$month': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dayOfMonthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_MONTH),
                of(utcDateTime.toInstant()).dayOfMonth(),
                "{'$dayOfMonth': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dayOfWeekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfWeek/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_WEEK) + 1,
                of(utcDateTime.toInstant()).dayOfWeek(),
                "{'$dayOfWeek': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dayOfYearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfYear/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_YEAR),
                of(utcDateTime.toInstant()).dayOfYear(),
                "{'$dayOfYear': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void hourTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/
        assertExpression(
                utcDateTime.get(ChronoField.HOUR_OF_DAY),
                of(utcDateTime.toInstant()).hour(),
                "{'$hour': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void minuteTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/minute/
        assertExpression(
                utcDateTime.get(ChronoField.MINUTE_OF_HOUR),
                of(utcDateTime.toInstant()).minute(),
                "{'$minute': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void secondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/second/
        assertExpression(
                utcDateTime.get(ChronoField.SECOND_OF_MINUTE),
                of(utcDateTime.toInstant()).second(),
                "{'$second': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void weekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/
        assertExpression(
                48,
                of(utcDateTime.toInstant()).week(),
                "{'$week': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void millisecondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/millisecond/
        assertExpression(
                utcDateTime.get(ChronoField.MILLI_OF_SECOND),
                of(utcDateTime.toInstant()).millisecond(),
                "{'$millisecond': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dateToStringTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        assertExpression(
                utcDateTime.toInstant().toString(),
                of(utcDateTime.toInstant()).dateToString());
        // with parameters
        assertExpression(
                utcDateTime.withZoneSameInstant(ZoneId.of("America/New_York")).format(ISO_LOCAL_DATE_TIME),
                of(utcDateTime.toInstant()).dateToString(of("%Y-%m-%dT%H:%M:%S.%L"), of("America/New_York")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L', "
                        + "'timezone': 'America/New_York'}}");
        assertExpression(
                utcDateTime.withZoneSameInstant(ZoneId.of("+04:30")).format(ISO_LOCAL_DATE_TIME),
                of(utcDateTime.toInstant()).dateToString(of("%Y-%m-%dT%H:%M:%S.%L"), of("+04:30")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%L', "
                        + "'timezone': '+04:30'}}");
    }
}

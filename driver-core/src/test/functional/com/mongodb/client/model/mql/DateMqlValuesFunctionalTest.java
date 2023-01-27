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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static com.mongodb.client.model.mql.MqlValues.of;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("ConstantConditions")
class DateMqlValuesFunctionalTest extends AbstractMqlValuesFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#date-expression-operators

    private final Instant instant = Instant.parse("2007-12-03T10:15:30.005Z");
    private final MqlDate date = of(instant);
    private final ZonedDateTime utcDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of(ZoneOffset.UTC.getId()));
    private final MqlString utc = of("UTC");

    @Test
    public void literalsTest() {
        assertExpression(
                instant,
                date,
                "{'$date': '2007-12-03T10:15:30.005Z'}");
        assertThrows(IllegalArgumentException.class, () -> of((Instant) null));
    }

    @Test
    public void yearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/
        assertExpression(
                utcDateTime.get(ChronoField.YEAR),
                date.year(utc),
                "{'$year': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void monthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/
        assertExpression(
                utcDateTime.get(ChronoField.MONTH_OF_YEAR),
                date.month(utc),
                "{'$month': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dayOfMonthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_MONTH),
                date.dayOfMonth(utc),
                "{'$dayOfMonth': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dayOfWeekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfWeek/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_WEEK) + 1,
                date.dayOfWeek(utc),
                "{'$dayOfWeek': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void dayOfYearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfYear/
        assertExpression(
                utcDateTime.get(ChronoField.DAY_OF_YEAR),
                date.dayOfYear(utc),
                "{'$dayOfYear': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void hourTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/
        assertExpression(
                utcDateTime.get(ChronoField.HOUR_OF_DAY),
                date.hour(utc),
                "{'$hour': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void minuteTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/minute/
        assertExpression(
                utcDateTime.get(ChronoField.MINUTE_OF_HOUR),
                date.minute(utc),
                "{'$minute': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void secondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/second/
        assertExpression(
                utcDateTime.get(ChronoField.SECOND_OF_MINUTE),
                date.second(utc),
                "{'$second': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void weekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/
        assertExpression(
                48,
                date.week(utc),
                "{'$week': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

    @Test
    public void millisecondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/millisecond/
        assertExpression(
                utcDateTime.get(ChronoField.MILLI_OF_SECOND),
                date.millisecond(utc),
                "{'$millisecond': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, 'timezone': 'UTC'}}");
    }

}

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

import static com.mongodb.client.model.expressions.Expressions.of;

class DateExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#date-expression-operators

    private final Instant instant = Instant.parse("2007-12-03T10:15:30.005Z");

    @Test
    public void literalsTest() {
        assertExpression(
                instant,
                of(instant),
                "{'$date': '2007-12-03T10:15:30.005Z'}");
    }

    @Test
    public void yearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/
        assertExpression(
                2007,
                of(instant).year(),
                "{'$year': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void monthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/
        assertExpression(
                12,
                of(instant).month(),
                "{'$month': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dayOfMonthTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/
        assertExpression(
                3,
                of(instant).dayOfMonth(),
                "{'$dayOfMonth': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dayOfWeekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfWeek/
        assertExpression(
                2,
                of(instant).dayOfWeek(),
                "{'$dayOfWeek': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dayOfYearTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfYear/
        assertExpression(
                337,
                of(instant).dayOfYear(),
                "{'$dayOfYear': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void hourTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/
        assertExpression(
                10,
                of(instant).hour(),
                "{'$hour': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void minuteTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/minute/
        assertExpression(
                15,
                of(instant).minute(),
                "{'$minute': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void secondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/second/
        assertExpression(
                30,
                of(instant).second(),
                "{'$second': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void weekTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/
        assertExpression(
                48,
                of(instant).week(),
                "{'$week': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void millisecondTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/millisecond/
        assertExpression(
                5,
                of(instant).millisecond(),
                "{'$millisecond': {'$date': '2007-12-03T10:15:30.005Z'}}");
    }

    @Test
    public void dateToStringTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        assertExpression(
                "2007-12-03T10:15:30.005Z",
                of(instant).dateToString());
        // with parameters
        assertExpression(
                "2007-12-03T05:15:30.005Z",
                of(instant).dateToString(of("%Y-%m-%dT%H:%M:%S.%LZ"), of("America/New_York")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%LZ', "
                        + "'timezone': 'America/New_York'}}");
        assertExpression(
                "2007-12-03T14:45:30.005Z",
                of(instant).dateToString(of("%Y-%m-%dT%H:%M:%S.%LZ"), of("+04:30")),
                "{'$dateToString': {'date': {'$date': '2007-12-03T10:15:30.005Z'}, "
                        + "'format': '%Y-%m-%dT%H:%M:%S.%LZ', "
                        + "'timezone': '+04:30'}}");
    }
}

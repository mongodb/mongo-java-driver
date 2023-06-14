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

package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TimeSeriesOptionsTest {

    private TimeSeriesOptions timeSeriesOptions;

    @BeforeEach
    void setUp() {
        timeSeriesOptions = new TimeSeriesOptions("test");
    }

    @Test
    void shouldThrowErrorWhenGranularityIsAlreadySet() {
        //given
        timeSeriesOptions.granularity(TimeSeriesGranularity.SECONDS);

        //when & then
        assertThrows(IllegalStateException.class, () -> timeSeriesOptions.bucketRounding(1L, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, () -> timeSeriesOptions.bucketMaxSpan(1L, TimeUnit.SECONDS));
    }

    @Test
    void shouldThrowErrorWhenGetWithNullParameter() {
        assertThrows(IllegalArgumentException.class, () -> timeSeriesOptions.getBucketMaxSpan(null));
        assertThrows(IllegalArgumentException.class, () -> timeSeriesOptions.getBucketRounding(null));
    }

    @ParameterizedTest
    @MethodSource("args")
    void shouldThrowErrorWhenInvalidArgumentProvided(@Nullable final Long valueToSet, @Nullable final TimeUnit timeUnit) {
        assertThrows(IllegalArgumentException.class, () -> timeSeriesOptions.bucketRounding(valueToSet, timeUnit));
        assertThrows(IllegalArgumentException.class, () -> timeSeriesOptions.bucketMaxSpan(valueToSet, timeUnit));
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                arguments(1L, null),
                arguments(null, null),
                arguments(1L, TimeUnit.MILLISECONDS)
        );
    }
}

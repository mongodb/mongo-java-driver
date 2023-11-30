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
package com.mongodb.internal.connection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class RoundTripTimeSamplerTest {

    @ParameterizedTest(name = "{index}: samples: {0}. Expected: average: {1} min: {2}")
    @DisplayName("RoundTripTimeSampler should calculate the expected average and min round trip times")
    @MethodSource
    public void testRoundTripTimeSampler(final List<Integer> samples, final int expectedAverageRTT, final int expectedMinRTT) {
        RoundTripTimeSampler sampler = new RoundTripTimeSampler();
        samples.forEach(sampler::addSample);

        assertEquals(expectedMinRTT, sampler.getMin());
        assertEquals(expectedAverageRTT, sampler.getAverage());
    }

    private static Stream<Arguments> testRoundTripTimeSampler() {
        return Stream.of(
                Arguments.of(emptyList(), 0, 0),
                Arguments.of(singletonList(10), 10, 0),
                Arguments.of(asList(10, 20), 12, 10),
                Arguments.of(asList(10, 20, 8), 11, 8),
                Arguments.of(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), 11, 6)
        );
    }

}

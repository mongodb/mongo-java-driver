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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class ExponentiallyWeightedMovingAverageTest {

    @ParameterizedTest(name = "{index}: {0}")
    @ValueSource(doubles = {-0.001, -0.01, -0.1, -1, 1.001, 1.01, 1.1})
    @DisplayName("constructor should throw if alpha is not between 0.0 and 1.0")
    void testInvalidAlpha(final double alpha) {
        assertThrows(IllegalArgumentException.class, () -> new ExponentiallyWeightedMovingAverage(alpha));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @ValueSource(doubles = {-0.0, 0.01, 0.1, 0.001, 0.01, 0.1, 0.2, 1.0})
    @DisplayName("constructor should not throw if alpha is between 0.0 and 1.0")
    void testValidAlpha(final double alpha) {
        assertDoesNotThrow(() -> new ExponentiallyWeightedMovingAverage(alpha));
    }


    @ParameterizedTest(name = "{index}: samples: {1}. Expected: {2}")
    @DisplayName("the average should be exponentially weighted")
    @MethodSource
    public void testAverageIsExponentiallyWeighted(final double alpha, final List<Integer> samples, final int expectedAverageRTT) {
        ExponentiallyWeightedMovingAverage average = new ExponentiallyWeightedMovingAverage(alpha);
        samples.forEach(average::addSample);

        assertEquals(expectedAverageRTT, average.getAverage());
    }

    private static Stream<Arguments> testAverageIsExponentiallyWeighted() {
        return Stream.of(
                Arguments.of(0.2, emptyList(), 0),
                Arguments.of(0.2, singletonList(10), 10),
                Arguments.of(0.2, asList(10, 20), 12),
                Arguments.of(0.2, asList(10, 20, 12), 12),
                Arguments.of(0.2, asList(10, 20, 12, 17), 13)
        );
    }

}

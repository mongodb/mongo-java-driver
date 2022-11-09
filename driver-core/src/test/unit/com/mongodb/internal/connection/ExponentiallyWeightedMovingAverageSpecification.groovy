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

package com.mongodb.internal.connection

import spock.lang.Specification


class ExponentiallyWeightedMovingAverageSpecification extends Specification {

    def 'constructor should throw if alpha is not between 0.0 and 1.0'() {
        when:
        new ExponentiallyWeightedMovingAverage(alpha)

        then:
        thrown(IllegalArgumentException)

        where:
        alpha << [-0.001, -0.01, -0.1, -1, 1.001, 1.01, 1.1]
    }

    def 'constructor should not throw if alpha is between 0.0 and 1.0'() {
        when:
        new ExponentiallyWeightedMovingAverage(alpha)

        then:
        true

        where:
        alpha << [-0.0, 0.01, 0.1, 0.001, 0.01, 0.1, 0.2, 1.0]
    }

    def 'the average should be exponentially weighted'() {
        when:
        def average = new ExponentiallyWeightedMovingAverage(alpha)
        for (def sample : samples) {
            average.addSample(sample)
        }

        then:
        average.getAverage() == result

        where:
        alpha << [0.2, 0.2, 0.2, 0.2, 0.2]
        samples << [[], [10], [10, 20], [10, 20, 12], [10, 20, 12, 17]]
        result << [0, 10, 12, 12, 13]
    }
}

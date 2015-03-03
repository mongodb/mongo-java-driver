package com.mongodb.connection

import spock.lang.Specification


class ExponentiallyWeightedMovingAverageSpecification extends Specification {

    def 'constructor should throw if alpha is not between 0.0 and 1.0'() {
        when:
        new ExponentiallyWeightedMovingAverage(alpha);

        then:
        thrown(IllegalArgumentException)

        where:
        alpha << [-0.001, -0.01, -0.1, -1, 1.001, 1.01, 1.1]
    }

    def 'constructor should not throw if alpha is between 0.0 and 1.0'() {
        when:
        new ExponentiallyWeightedMovingAverage(alpha);

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

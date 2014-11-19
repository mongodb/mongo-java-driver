package com.mongodb.connection;

import com.mongodb.annotations.NotThreadSafe;

import static com.mongodb.assertions.Assertions.isTrueArgument;

@NotThreadSafe
class ExponentiallyWeightedMovingAverage {
    private final double alpha;
    private long average;

    ExponentiallyWeightedMovingAverage(final double alpha) {
        isTrueArgument("alpha >= 0.0 and <= 1.0", alpha >= 0.0 && alpha <= 1.0);
        this.alpha = alpha;
    }

    void reset() {
        average = 0;
    }

    long addSample(final long sample) {
        if (average == 0) {
            average = sample;
        } else {
            average = (long) (alpha * sample + (1 - alpha) * average);
        }

        return average;
    }

    long getAverage() {
        return average;
    }
}

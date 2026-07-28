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

/**
 * Normalization methods for the {@link Aggregates#score(Object, ScoreOptions) $score}
 * and {@code $scoreFusion} pipeline stages.
 *
 * @mongodb.driver.manual reference/operator/aggregation/score/ $score
 * @mongodb.driver.manual reference/operator/aggregation/scoreFusion/ $scoreFusion
 * @mongodb.server.release 8.2
 * @since 5.10
 */
public enum ScoreNormalization {
    /**
     * No normalization is applied.
     */
    NONE("none"),
    /**
     * Normalizes the score to the range (0, 1) by applying the sigmoid function.
     */
    SIGMOID("sigmoid"),
    /**
     * Normalizes the score to the range [0, 1] by applying min-max scaling.
     */
    MIN_MAX_SCALER("minMaxScaler");

    private final String value;

    ScoreNormalization(final String value) {
        this.value = value;
    }

    /**
     * Returns the value as expected by the server.
     *
     * @return the server value
     */
    public String getValue() {
        return value;
    }
}

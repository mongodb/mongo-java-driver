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

import org.bson.conversions.Bson;

/**
 * The options for a {@link Aggregates#score(Object, ScoreOptions) $score} pipeline stage.
 *
 * @mongodb.driver.manual reference/operator/aggregation/score/ $score
 * @mongodb.server.release 8.2
 * @since 5.10
 */
public interface ScoreOptions extends Bson {
    /**
     * Returns {@link ScoreOptions} that represents server defaults.
     *
     * @return {@link ScoreOptions} that represents server defaults.
     */
    static ScoreOptions scoreOptions() {
        return ScoreConstructibleBson.EMPTY_IMMUTABLE;
    }

    /**
     * The method used to normalize the score to the range [0, 1].
     * If this option is not provided, the server default is {@link ScoreNormalization#NONE}.
     *
     * @param normalization the normalization method
     * @return a new {@link ScoreOptions} with the provided option set
     * @since 5.10
     */
    ScoreOptions normalization(ScoreNormalization normalization);

    /**
     * The factor to multiply the score by after normalization.
     * Must be in the range [0, 1].
     *
     * @param weight the weight
     * @return a new {@link ScoreOptions} with the provided option set
     * @since 5.10
     */
    ScoreOptions weight(double weight);

    /**
     * Specifies whether to populate the {@code scoreDetails} metadata field,
     * which contains details on how the score was computed.
     * If this option is not provided, the server default is {@code false}.
     *
     * @param scoreDetails whether to populate the {@code scoreDetails} metadata field
     * @return a new {@link ScoreOptions} with the provided option set
     * @since 5.10
     */
    ScoreOptions scoreDetails(boolean scoreDetails);
}

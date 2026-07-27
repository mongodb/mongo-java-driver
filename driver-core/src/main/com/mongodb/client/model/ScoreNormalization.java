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

import com.mongodb.annotations.Sealed;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The way in which the scores produced by the {@linkplain Aggregates#scoreFusion(java.util.List, ScoreNormalization,
 * ScoreFusionOptions) $scoreFusion} input pipelines are normalized before being combined.
 *
 * @since 5.10
 * @mongodb.server.release 8.2
 */
@Sealed
public interface ScoreNormalization {
    /**
     * Returns a {@link ScoreNormalization} instance representing no normalization.
     *
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization none() {
        return new ScoreNormalizationBson(new BsonString("none"));
    }

    /**
     * Returns a {@link ScoreNormalization} instance representing normalization via the sigmoid function.
     *
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization sigmoid() {
        return new ScoreNormalizationBson(new BsonString("sigmoid"));
    }

    /**
     * Returns a {@link ScoreNormalization} instance representing min-max scaling of the scores to the range [0, 1].
     *
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization minMaxScaler() {
        return new ScoreNormalizationBson(new BsonString("minMaxScaler"));
    }

    /**
     * Creates a {@link ScoreNormalization} from a {@link BsonValue} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     *
     * @param normalization A {@link BsonValue} representing the required {@link ScoreNormalization}.
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization of(final BsonValue normalization) {
        return new ScoreNormalizationBson(notNull("normalization", normalization));
    }

    /**
     * Converts this object to {@link BsonValue}.
     *
     * @return A {@link BsonValue} representing this {@link ScoreNormalization}.
     */
    BsonValue toBsonValue();
}

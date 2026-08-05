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
import org.bson.conversions.Bson;

/**
 * Represents optional fields of the {@linkplain Aggregates#scoreFusion(java.util.List, ScoreNormalization,
 * ScoreFusionOptions) $scoreFusion} pipeline stage of an aggregation pipeline.
 *
 * @since 5.10
 * @mongodb.driver.manual reference/operator/aggregation/scoreFusion/ $scoreFusion
 * @mongodb.server.release 8.2
 */
@Sealed
public interface ScoreFusionOptions extends Bson {
    /**
     * Returns {@link ScoreFusionOptions} that represents server defaults.
     *
     * @return {@link ScoreFusionOptions} that represents server defaults.
     */
    static ScoreFusionOptions scoreFusionOptions() {
        return ScoreFusionConstructibleBson.EMPTY_IMMUTABLE;
    }

    /**
     * Creates a new {@link ScoreFusionOptions} with the combination specified.
     * If not specified, the server combines the normalized scores using its default method.
     *
     * @param combination The way in which the normalized scores are combined.
     * @return A new {@link ScoreFusionOptions}.
     */
    ScoreFusionOptions combination(ScoreFusionCombination combination);

    /**
     * Creates a new {@link ScoreFusionOptions} with the scoreDetails flag specified.
     * When {@code true}, the server exposes score details via the {@code {$meta: "scoreDetails"}} expression.
     * Server default is {@code false}.
     *
     * @param scoreDetails Whether to include score details.
     * @return A new {@link ScoreFusionOptions}.
     */
    ScoreFusionOptions scoreDetails(boolean scoreDetails);

    /**
     * Creates a new {@link ScoreFusionOptions} with the specified option in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     *
     * @param name The option name.
     * @param value The option value.
     * @return A new {@link ScoreFusionOptions}.
     */
    ScoreFusionOptions option(String name, Object value);
}

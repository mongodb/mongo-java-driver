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
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The way in which the normalized scores produced by the input pipelines of the
 * {@linkplain Aggregates#scoreFusion(java.util.List, ScoreNormalization, ScoreFusionOptions) $scoreFusion}
 * stage are combined into the final score. The server rejects specifying both
 * {@linkplain #weighted(Bson) weights} and an {@linkplain #expression(Bson) expression},
 * which is why they are separate factory methods.
 *
 * @see ScoreFusionOptions#combination(ScoreFusionCombination)
 * @since 5.10
 * @mongodb.server.release 8.2
 */
@Sealed
public interface ScoreFusionCombination extends Bson {
    /**
     * Returns a {@link WeightedScoreFusionCombination} combining the scores using per-pipeline weights.
     *
     * @param weights A document mapping {@linkplain FusionPipeline#getName() pipeline names} to non-negative
     * numeric weights. Pipelines not mentioned have the server-default weight 1.
     * @return The requested {@link WeightedScoreFusionCombination}.
     */
    static WeightedScoreFusionCombination weighted(final Bson weights) {
        return new ScoreFusionConstructibleBson(new Document("weights", notNull("weights", weights)));
    }

    /**
     * Returns a {@link ScoreFusionCombination} combining the scores using a custom expression.
     * The normalized, weighted score of each input pipeline is available to the expression
     * as the variable named after the pipeline, e.g., {@code "$$name"}.
     *
     * @param expression The combination expression.
     * @return The requested {@link ScoreFusionCombination}.
     */
    static ScoreFusionCombination expression(final Bson expression) {
        return new ScoreFusionConstructibleBson(new Document("method", new BsonString("expression"))
                .append("expression", notNull("expression", expression)));
    }
}

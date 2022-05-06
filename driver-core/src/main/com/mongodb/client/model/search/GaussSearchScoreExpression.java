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
package com.mongodb.client.model.search;

import com.mongodb.annotations.Evolving;

/**
 * @see SearchScoreExpression#gaussExpression(double, PathSearchScoreExpression, double)
 * @since 4.7
 */
@Evolving
public interface GaussSearchScoreExpression extends SearchScoreExpression {
    /**
     * Creates a new {@link GaussSearchScoreExpression} which does not decay, i.e., its output stays 1, if the value of the
     * {@link SearchScoreExpression#gaussExpression(double, PathSearchScoreExpression, double) path} expression is within the interval
     * [{@link SearchScoreExpression#gaussExpression(double, PathSearchScoreExpression, double) origin} - {@code offset};
     * {@code origin} + {@code offset}].
     *
     * @param offset The offset from the origin where no decay happens.
     * @return A new {@link GaussSearchScoreExpression}.
     */
    GaussSearchScoreExpression offset(double offset);

    /**
     * Creates a new {@link GaussSearchScoreExpression} with the factor by which the output of the Gaussian function must decay at distance
     * {@link SearchScoreExpression#gaussExpression(double, PathSearchScoreExpression, double) scale}.
     *
     * @param decay The decay.
     * @return A new {@link GaussSearchScoreExpression}.
     */
    GaussSearchScoreExpression decay(double decay);
}

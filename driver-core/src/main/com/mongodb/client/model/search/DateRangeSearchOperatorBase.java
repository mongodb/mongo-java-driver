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
 * WITHOUNumber WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.client.model.search;

import com.mongodb.annotations.Evolving;

import java.time.Instant;

/**
 * A base for a {@link DateRangeSearchOperator} which allows creating instances of this operator.
 * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
 *
 * @see SearchOperator#dateRange(FieldSearchPath)
 * @see SearchOperator#dateRange(Iterable)
 * @since 4.7
 */
@Evolving
public interface DateRangeSearchOperatorBase {
    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within (l; ∞).
     *
     * @param l The lower bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator gt(Instant l);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within (-∞; u).
     *
     * @param u The upper bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator lt(Instant u);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within [l; ∞).
     *
     * @param l The lower bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator gte(Instant l);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within (-∞; u].
     *
     * @param u The upper bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator lte(Instant u);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within (l; u).
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator gtLt(Instant l, Instant u);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within [l; u].
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator gteLte(Instant l, Instant u);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within (l; u].
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator gtLte(Instant l, Instant u);

    /**
     * Creates a new {@link DateRangeSearchOperator} that tests if values are within [l; u).
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link DateRangeSearchOperator}.
     */
    DateRangeSearchOperator gteLt(Instant l, Instant u);
}

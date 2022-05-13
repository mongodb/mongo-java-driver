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
 * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
 *
 * @param <T> The type of the bounds.
 * @see SearchOperator#numberRange(FieldSearchPath)
 * @see SearchOperator#numberRange(Iterable)
 * @see SearchOperator#dateRange(FieldSearchPath)
 * @see SearchOperator#dateRange(Iterable)
 * @since 4.7
 */
@Evolving
public interface RangeSearchOperatorBase<T> {
    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within (l; ∞).
     *
     * @param l The lower bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> gt(T l);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within (-∞; u).
     *
     * @param u The upper bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> lt(T u);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within [l; ∞).
     *
     * @param l The lower bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> gte(T l);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within (-∞; u].
     *
     * @param u The upper bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> lte(T u);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within (l; u).
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> gtLt(T l, T u);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within [l; u].
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> gteLte(T l, T u);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within (l; u].
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> gtLte(T l, T u);

    /**
     * Creates a new {@link RangeSearchOperator} that tests if values are within [l; u).
     *
     * @param l The lower bound.
     * @param u The upper bound.
     * @return A new {@link RangeSearchOperator}.
     */
    RangeSearchOperator<T> gteLt(T l, T u);
}

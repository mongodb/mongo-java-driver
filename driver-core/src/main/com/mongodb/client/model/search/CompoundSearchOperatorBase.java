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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Reason;
import com.mongodb.annotations.Sealed;

/**
 * A base for a {@link CompoundSearchOperator} which allows creating instances of this operator.
 * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
 *
 * @see SearchOperator#compound()
 * @since 4.7
 */
@Sealed
@Beta(Reason.CLIENT)
public interface CompoundSearchOperatorBase {
    /**
     * Creates a new {@link CompoundSearchOperator} by adding to it {@code clauses} that must all be satisfied.
     * <p>
     * This method may be called multiple times.</p>
     *
     * @param clauses The non-empty clauses to add.
     * @return A new {@link CompoundSearchOperator}.
     */
    MustCompoundSearchOperator must(Iterable<? extends SearchOperator> clauses);

    /**
     * Creates a new {@link CompoundSearchOperator} by adding to it {@code clauses} that must all not be satisfied.
     * <p>
     * This method may be called multiple times.</p>
     *
     * @param clauses The non-empty clauses to add.
     * @return A new {@link CompoundSearchOperator}.
     */
    MustNotCompoundSearchOperator mustNot(Iterable<? extends SearchOperator> clauses);

    /**
     * Creates a new {@link CompoundSearchOperator} by adding to it {@code clauses} that are preferred to be satisfied.
     * <p>
     * This method may be called multiple times.</p>
     *
     * @param clauses The non-empty clauses to add.
     * @return A new {@link CompoundSearchOperator}.
     */
    ShouldCompoundSearchOperator should(Iterable<? extends SearchOperator> clauses);

    /**
     * Creates a new {@link CompoundSearchOperator} by adding to it {@code clauses} that, similarly to {@link #must(Iterable)},
     * must all be satisfied. The difference is that this method does not affect the relevance score.
     * <p>
     * This method may be called multiple times.</p>
     *
     * @param clauses The non-empty clauses to add.
     * @return A new {@link CompoundSearchOperator}.
     */
    FilterCompoundSearchOperator filter(Iterable<? extends SearchOperator> clauses);
}

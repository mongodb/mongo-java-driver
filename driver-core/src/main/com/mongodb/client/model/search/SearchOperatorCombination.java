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
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.client.model.Util.sizeAtLeast;
import static org.bson.assertions.Assertions.notNull;

/**
 * Represents a combination of {@link SearchOperator}s with a rule affecting how they are used when matching documents
 * and calculating the relevance score assigned to each found document.
 * These {@link SearchOperator}s are called "clauses" in the context of a {@link CompoundSearchOperator}.
 *
 * @see SearchOperator#compound(Iterable)
 * @since 4.7
 */
@Evolving
public interface SearchOperatorCombination extends Bson {
    /**
     * Returns a {@link SearchOperatorCombination} of clauses that must all be satisfied.
     *
     * @param clauses Non-empty clauses.
     * @return The requested {@link SearchOperatorCombination}.
     */
    static MustSearchOperatorCombination must(final Iterable<? extends SearchOperator> clauses) {
        notNull("clauses", clauses);
        isTrueArgument("clauses must not be empty", sizeAtLeast(clauses, 1));
        return new SearchConstructibleBson(new Document("must", clauses));
    }

    /**
     * Returns a {@link SearchOperatorCombination} of clauses that must all not be satisfied.
     *
     * @param clauses Non-empty clauses.
     * @return The requested {@link SearchOperatorCombination}.
     */
    static MustNotSearchOperatorCombination mustNot(final Iterable<? extends SearchOperator> clauses) {
        notNull("clauses", clauses);
        isTrueArgument("clauses must not be empty", sizeAtLeast(clauses, 1));
        return new SearchConstructibleBson(new Document("mustNot", clauses));
    }

    /**
     * Returns a {@link SearchOperatorCombination} of clauses that are preferred to be satisfied.
     *
     * @param clauses Non-empty clauses.
     * @return The requested {@link SearchOperatorCombination}.
     */
    static ShouldSearchOperatorCombination should(final Iterable<? extends SearchOperator> clauses) {
        notNull("clauses", clauses);
        isTrueArgument("clauses must not be empty", sizeAtLeast(clauses, 1));
        return new SearchConstructibleBson(new Document("should", clauses));
    }

    /**
     * Returns a {@link SearchOperatorCombination} of clauses that, similarly to {@link #must(Iterable)}, must all be satisfied.
     * The difference is that {@link #filter(Iterable)} does not affect the relevance score.
     *
     * @param clauses Non-empty clauses.
     * @return The requested {@link SearchOperatorCombination}.
     */
    static FilterSearchOperatorCombination filter(final Iterable<? extends SearchOperator> clauses) {
        notNull("clauses", clauses);
        isTrueArgument("clauses must not be empty", sizeAtLeast(clauses, 1));
        return new SearchConstructibleBson(new Document("filter", clauses));
    }
}

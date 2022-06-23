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
import com.mongodb.annotations.Evolving;

/**
 * @see SearchOperator#autocomplete(FieldSearchPath, String, String...)
 * @see SearchOperator#autocomplete(FieldSearchPath, Iterable)
 * @since 4.7
 */
@Evolving
@Beta(Beta.Reason.CLIENT)
public interface AutocompleteSearchOperator extends SearchOperator {
    @Override
    AutocompleteSearchOperator score(SearchScore modifier);

    /**
     * Creates a new {@link AutocompleteSearchOperator} that uses fuzzy search.
     *
     * @return A new {@link AutocompleteSearchOperator}.
     */
    AutocompleteSearchOperator fuzzy();

    /**
     * Creates a new {@link AutocompleteSearchOperator} that uses fuzzy search.
     *
     * @param options The fuzzy search options.
     * Specifying {@link FuzzySearchOptions#fuzzySearchOptions()} is equivalent to calling {@link #fuzzy()}.
     * @return A new {@link AutocompleteSearchOperator}.
     */
    AutocompleteSearchOperator fuzzy(FuzzySearchOptions options);

    /**
     * Creates a new {@link AutocompleteSearchOperator} that does not require tokens to appear in the same order as they are specified.
     *
     * @return A new {@link AutocompleteSearchOperator}.
     * @see #sequentialTokenOrder()
     */
    AutocompleteSearchOperator anyTokenOrder();

    /**
     * Creates a new {@link AutocompleteSearchOperator} that requires tokens to appear in the same order as they are specified.
     *
     * @return A new {@link AutocompleteSearchOperator}.
     * @see #anyTokenOrder()
     */
    AutocompleteSearchOperator sequentialTokenOrder();
}

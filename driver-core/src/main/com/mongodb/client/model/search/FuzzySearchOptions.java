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
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Fuzzy search options that may be used with some {@link SearchOperator}s.
 *
 * @mongodb.atlas.manual atlas-search/autocomplete/ autocomplete operator
 * @mongodb.atlas.manual atlas-search/text/ text operator
 * @since 4.7
 */
@Evolving
@Beta(Beta.Reason.CLIENT)
public interface FuzzySearchOptions extends Bson {
    /**
     * Creates a new {@link FuzzySearchOptions} with the maximum
     * <a href="https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance">number of single-character edits</a>
     * required to match a search term.
     *
     * @param maxEdits The maximum number of single-character edits required to match a search term.
     * @return A new {@link FuzzySearchOptions}.
     */
    FuzzySearchOptions maxEdits(int maxEdits);

    /**
     * Creates a new {@link FuzzySearchOptions} with the number of characters at the beginning of a search term that must exactly match.
     *
     * @param prefixLength The number of characters at the beginning of a search term that must exactly match.
     * @return A new {@link FuzzySearchOptions}.
     */
    FuzzySearchOptions prefixLength(int prefixLength);

    /**
     * Creates a new {@link FuzzySearchOptions} with the maximum number of variations to generate and consider to match a search term.
     *
     * @param maxExpansions The maximum number of variations to generate and consider to match a search term.
     * @return A new {@link FuzzySearchOptions}.
     */
    FuzzySearchOptions maxExpansions(int maxExpansions);

    /**
     * Creates a new {@link FuzzySearchOptions} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link FuzzySearchOptions} objects,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  FuzzySearchOptions fuzzy1 = FuzzySearchOptions.fuzzySearchOptions().maxEdits(1);
     *  FuzzySearchOptions fuzzy2 = FuzzySearchOptions.of(new Document("maxEdits", 1));
     * }</pre>
     *
     * @param fuzzy A {@link Bson} representing the required {@link FuzzySearchOptions}.
     * @return A new {@link FuzzySearchOptions}.
     */
    static FuzzySearchOptions of(final Bson fuzzy) {
        return new SearchConstructibleBson(notNull("fuzzy", fuzzy));
    }

    /**
     * Returns {@link FuzzySearchOptions} that represents server defaults.
     *
     * @return {@link FuzzySearchOptions} that represents server defaults.
     */
    static FuzzySearchOptions fuzzySearchOptions() {
        return SearchConstructibleBson.EMPTY_IMMUTABLE;
    }
}

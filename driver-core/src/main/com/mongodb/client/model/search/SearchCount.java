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
import com.mongodb.client.model.Projections;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Counting options.
 * You may use the {@code $$SEARCH_META} variable, e.g., via {@link Projections#computedSearchMeta(String)},
 * to extract the results of counting.
 *
 * @mongodb.atlas.manual atlas-search/counting/ Counting
 * @since 4.7
 */
@Evolving
@Beta({Beta.Reason.CLIENT, Beta.Reason.SERVER})
public interface SearchCount extends Bson {
    /**
     * Returns a {@link SearchCount} that instructs to count documents exactly.
     *
     * @return The requested {@link SearchCount}.
     */
    static TotalSearchCount total() {
        return new SearchConstructibleBson(new BsonDocument("type", new BsonString("total")));
    }

    /**
     * Returns a {@link SearchCount} that instructs to count documents exactly only up to a
     * {@linkplain LowerBoundSearchCount#threshold(int) threshold}.
     *
     * @return The requested {@link SearchCount}.
     */
    static LowerBoundSearchCount lowerBound() {
        return new SearchConstructibleBson(new BsonDocument("type", new BsonString("lowerBound")));
    }

    /**
     * Creates a {@link SearchCount} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchCount}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchCount count1 = SearchCount.lowerBound();
     *  SearchCount count2 = SearchCount.of(new Document("type", "lowerBound"));
     * }</pre>
     *
     * @param count A {@link Bson} representing the required {@link SearchCount}.
     * @return The requested {@link SearchCount}.
     */
    static SearchCount of(final Bson count) {
        return new SearchConstructibleBson(notNull("count", count));
    }
}

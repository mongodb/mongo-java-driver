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
import com.mongodb.client.model.Projections;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.Iterator;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.client.model.BsonUtil.toBsonValue;

/**
 * Highlighting options.
 * You may use the {@code $meta: "searchHighlights"} expression, e.g., via {@link Projections#metaSearchHighlights(String)},
 * to extract the results of highlighting.
 *
 * @mongodb.atlas.manual atlas-search/highlighting/ Highlighting
 * @since 4.6
 */
@Evolving
public interface SearchHighlight extends Bson {
    /**
     * Creates a new {@link SearchHighlight} with the maximum number of characters to examine on a document
     * when performing highlighting for a field.
     *
     * @param maxCharsToExamine The maximum number of characters to examine.
     * @return A new {@link SearchHighlight}.
     */
    SearchHighlight maxCharsToExamine(int maxCharsToExamine);

    /**
     * Creates a new {@link SearchHighlight} with the maximum number of high-scoring passages to return per document
     * in the {@code "highlights"} results for each field.
     *
     * @param maxNumPassages The maximum number of high-scoring passages.
     * @return A new {@link SearchHighlight}.
     */
    SearchHighlight maxNumPassages(int maxNumPassages);

    /**
     * Returns a {@link SearchHighlight} for the given {@code paths}.
     *
     * @param paths Non-empty document fields to be searched.
     * @return The requested {@link SearchHighlight}.
     */
    static SearchHighlight paths(final Iterable<? extends SearchPath> paths) {
        Iterator<? extends SearchPath> pathIterator = notNull("paths", paths).iterator();
        if (!pathIterator.hasNext()) {
            throw new IllegalArgumentException("paths must not be empty");
        }
        return new SearchConstructibleBson(new BsonDocument("path", toBsonValue(pathIterator)));
    }

    /**
     * Creates a {@link SearchHighlight} from a {@link Bson} in situations when there is no builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link SearchHighlight}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  SearchHighlight highlight1 = SearchHighlight.paths(Arrays.asList(
     *          SearchPath.fieldPath("fieldName"),
     *          SearchPath.wildcardPath("wildc*rd")));
     *  SearchHighlight highlight2 = SearchHighlight.of(new BsonDocument("path", new BsonArray(Arrays.asList(
     *          SearchPath.fieldPath("fieldName").toBsonValue(),
     *          SearchPath.wildcardPath("wildc*rd").toBsonValue()))));
     * }</pre>
     *
     * @param highlight A {@link Bson} representing the required {@link SearchHighlight}.
     * @return The requested {@link SearchHighlight}.
     */
    static SearchHighlight of(final Bson highlight) {
        return new SearchConstructibleBson(notNull("highlight", highlight));
    }
}

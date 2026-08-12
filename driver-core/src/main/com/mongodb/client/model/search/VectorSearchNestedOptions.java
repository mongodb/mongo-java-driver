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

import com.mongodb.annotations.Sealed;
import org.bson.conversions.Bson;

/**
 * Represents the optional {@code nestedOptions} sub-document of the {@code $vectorSearch} pipeline stage,
 * used when searching against arrays of embeddings within nested (embedded) documents.
 *
 * @see VectorSearchOptions#nestedOptions(VectorSearchNestedOptions)
 * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
 * @mongodb.server.release 8.3
 * @since 5.10
 */
@Sealed
public interface VectorSearchNestedOptions extends Bson {
    /**
     * Creates a new {@link VectorSearchNestedOptions} with the score aggregation mode specified.
     *
     * @param scoreMode The score aggregation mode for the matching embeddings within a document.
     * @return A new {@link VectorSearchNestedOptions}.
     */
    VectorSearchNestedOptions scoreMode(VectorSearchScoreMode scoreMode);

    /**
     * Creates a new {@link VectorSearchNestedOptions} with the specified option in situations when there is no
     * builder method that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link VectorSearchNestedOptions} objects,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  VectorSearchNestedOptions options1 = VectorSearchNestedOptions.vectorSearchNestedOptions()
     *      .scoreMode(VectorSearchScoreMode.AVG);
     *  VectorSearchNestedOptions options2 = VectorSearchNestedOptions.vectorSearchNestedOptions()
     *      .option("scoreMode", "avg");
     * }</pre>
     *
     * @param name The option name.
     * @param value The option value.
     * @return A new {@link VectorSearchNestedOptions}.
     */
    VectorSearchNestedOptions option(String name, Object value);

    /**
     * Returns {@link VectorSearchNestedOptions} that represents server defaults.
     *
     * @return {@link VectorSearchNestedOptions} that represents server defaults.
     */
    static VectorSearchNestedOptions vectorSearchNestedOptions() {
        return VectorSearchNestedConstructibleBson.EMPTY_IMMUTABLE;
    }
}

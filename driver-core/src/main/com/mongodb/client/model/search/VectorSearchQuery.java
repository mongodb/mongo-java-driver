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
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A query specification for MongoDB Atlas vector search with automated embedding.
 * <p>
 * This interface provides factory methods for creating type-safe query objects that can be used
 * with the {@code $vectorSearch} aggregation pipeline stage for auto-embedding functionality.
 * </p>
 *
 * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
 * @since 5.7.0
 */
@Sealed
@Beta(Reason.SERVER)
public interface VectorSearchQuery extends Bson {
    /**
     * Creates a text-based vector search query that will be automatically embedded by the server.
     * <p>
     * The server will generate embeddings for the provided text using the model specified in the
     * vector search index definition, or an explicitly specified model via {@link TextVectorSearchQuery#model(String)}.
     * </p>
     *
     * @param text The text to be embedded and searched.
     * @return A {@link TextVectorSearchQuery} that can be further configured.
     */
    static TextVectorSearchQuery textQuery(final String text) {
        return new TextVectorSearchQueryImpl(notNull("text", text), null);
    }
}

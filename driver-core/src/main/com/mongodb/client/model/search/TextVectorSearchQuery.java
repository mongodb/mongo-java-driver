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
import com.mongodb.lang.Nullable;

/**
 * A text-based vector search query for MongoDB Atlas auto-embedding.
 * <p>
 * This interface extends {@link VectorSearchQuery} and provides methods for configuring
 * text-based queries that will be automatically embedded by the server.
 * </p>
 *
 * @see VectorSearchQuery#textQuery(String)
 * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
 * @since 5.7.0
 */
@Sealed
@Beta(Reason.SERVER)
public interface TextVectorSearchQuery extends VectorSearchQuery {
    /**
     * Specifies the embedding model to use for generating embeddings from the query text.
     * <p>
     * If not specified, the model configured in the vector search index definition will be used.
     * The specified model must be compatible with the model used in the index definition.
     * </p>
     *
     * @param modelName The name of the embedding model to use (e.g., "voyage-4-large").
     * @return A new {@link TextVectorSearchQuery} with the specified model.
     */
    TextVectorSearchQuery model(String modelName);

    /**
     * Returns the embedding model name, if specified.
     *
     * @return The model name, or {@code null} if not specified.
     */
    @Nullable
    String getModel();
}

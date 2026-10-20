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

/**
 * The score aggregation mode for a {@code $vectorSearch} against arrays of embeddings in nested (embedded) documents.
 *
 * <p>If {@code scoreMode} is not specified, the server default is {@link #MAX} ({@code "max"}).</p>
 *
 * @see VectorSearchNestedOptions#scoreMode(VectorSearchScoreMode)
 * @mongodb.atlas.manual atlas-vector-search/vector-search-stage/ $vectorSearch
 * @mongodb.server.release 8.3
 * @since 5.10
 */
public enum VectorSearchScoreMode {
    /**
     * Use the average of the scores of the matching embeddings within a document.
     */
    AVG("avg"),

    /**
     * Use the maximum of the scores of the matching embeddings within a document.
     */
    MAX("max");

    private final String value;

    VectorSearchScoreMode(final String value) {
        this.value = value;
    }

    /**
     * Returns the value used by the server for this score mode.
     *
     * @return the server value ({@code "avg"} or {@code "max"}).
     * @since 5.10
     */
    public String getValue() {
        return value;
    }
}

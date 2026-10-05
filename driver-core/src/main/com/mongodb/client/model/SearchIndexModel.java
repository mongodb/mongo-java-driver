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

package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing the creation of a single Atlas Search index.
 *
 * <p>The {@code definition} parameter accepts any {@link org.bson.conversions.Bson} instance.
 * For vector search indexes, use the builders provided by {@link SearchIndexDefinition#vectorSearch(Bson...)}
 * and {@link VectorSearchIndexFields} to construct the definition, and pass it to the
 * {@linkplain #SearchIndexModel(String, VectorSearchIndexDefinition) vector search constructor}
 * which automatically sets the index type to {@link SearchIndexType#vectorSearch()}.</p>
 *
 * @see SearchIndexDefinition
 * @see VectorSearchIndexFields
 * @since 4.11
 * @mongodb.server.release 6.0
 */
public final class SearchIndexModel {
    @Nullable
    private final String name;
    private final Bson definition;
    @Nullable
    private final SearchIndexType type;

    /**
     * Construct an instance with the given Atlas Search index mapping definition.
     *
     * <p>After calling this constructor, the {@code name} field will be {@code null}. In that case, when passing this
     * {@code SearchIndexModel} to the {@code createSearchIndexes} method, the default search index name 'default'
     * will be used to create the search index.</p>
     *
     * @param definition the search index mapping definition.
     * @see SearchIndexDefinition#vectorSearch(Bson...)
     */
    public SearchIndexModel(final Bson definition) {
        this(null, definition, null);
    }

    /**
     * Construct an instance with the given Atlas Search name and index definition.
     *
     * @param name       the search index name.
     * @param definition the search index mapping definition.
     * @see SearchIndexDefinition#vectorSearch(Bson...)
     */
    public SearchIndexModel(final String name, final Bson definition) {
        this(name, definition, null);
    }

    /**
     * Construct a vector search index instance with the given name and definition.
     *
     * <p>The index type is automatically set to {@link SearchIndexType#vectorSearch()}.</p>
     *
     * @param name       the search index name.
     * @param definition the vector search index definition.
     * @see SearchIndexDefinition#vectorSearch(Bson...)
     * @since 5.8
     */
    public SearchIndexModel(final String name, final VectorSearchIndexDefinition definition) {
        this(name, definition, SearchIndexType.vectorSearch());
    }

    /**
     * Construct an instance with the given Atlas Search name, index definition, and type.
     *
     * @param name       the search index name.
     * @param definition the search index mapping definition.
     * @param type       the search index type.
     * @see SearchIndexDefinition#vectorSearch(Bson...)
     * @since 5.2
     */
    public SearchIndexModel(@Nullable final String name, final Bson definition, @Nullable final SearchIndexType type) {
        this.definition = notNull("definition", definition);
        this.name = name;
        this.type = type;
    }

    /**
     * Get the Atlas Search index mapping definition.
     *
     * @return the index definition.
     */
    public Bson getDefinition() {
        return definition;
    }

    /**
     * Get the Atlas Search index name.
     *
     * @return the search index name.
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * Get the Atlas Search index type.
     *
     * @return the search index type.
     * @since 5.2
     */
    @Nullable
    public SearchIndexType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "SearchIndexModel{"
                + "name=" + name
                + ", definition=" + definition
                + ", type=" + (type == null ? "null" : type.toBsonValue())
                + '}';
    }
}

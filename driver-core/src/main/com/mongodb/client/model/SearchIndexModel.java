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
 * @since 4.11
 * @mongodb.server.release 7.0
 */
public final class SearchIndexModel {
    @Nullable
    private final String name;
    private final Bson definition;

    /**
     * Construct an instance with the given Atlas Search index definition.
     *
     * <p>After calling this constructor, the {@code name} field will be {@code null}. In that case, when passing this
     * {@code SearchIndexModel} to the {@code createSearchIndexes} method, the default search index name 'default'
     * will be used to create the search index.</p>
     *
     * @param definition the search index definition.
     */
    public SearchIndexModel(final Bson definition) {
        this(null, notNull("definition", definition));
    }

    /**
     * Construct an instance with the given Atlas Search name and index definition.
     *
     * @param name       the search index name.
     * @param definition the search index definition.
     */
    public SearchIndexModel(final String name, final Bson definition) {
        this.definition = notNull("definition", definition);
        this.name = name;
    }

    /**
     * Get the Atlas Search index definition.
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

    @Override
    public String toString() {
        return "SearchIndexModel{"
                + "name=" + name
                + ", definition=" + definition
                + '}';
    }
}

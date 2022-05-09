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

/**
 * Options for cluster index on a collection.
 *
 * @see CreateCollectionOptions
 * @since 4.7
 * @mongodb.server.release 5.3
 */
public class ClusteredIndexOptions {
    private final Bson key;
    private final boolean unique;
    private String name;

    /**
     * Construct an instance with the required options.
     *
     * @param key the index key, which currently must be {@code {_id: 1}}
     * @param unique whether the index entries must be unique, which currently must be true
     */
    public ClusteredIndexOptions(final Bson key, final boolean unique) {
        this.key = key;
        this.unique = unique;
    }

    /**
     * Gets the index key.
     *
     * @return the index key
     */
    public Bson getKey() {
        return key;
    }

    /**
     * Gets whether the index entries must be unique
     * @return whether the index entries must be unique
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Gets the index name
     *
     * @return the index name
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * Sets the index name
     * @param name the index name
     */
    public void name(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "ClusteredIndexOptions{"
                + "key=" + key
                + ", unique=" + unique
                + ", name='" + name + '\''
                + '}';
    }
}

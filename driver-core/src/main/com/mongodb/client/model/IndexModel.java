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

import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing the creation of a single index.
 *
 * @mongodb.driver.manual reference/command/createIndexes Index options
 * @since 3.0
 */
public class IndexModel {
    private final Bson keys;
    private final IndexOptions options;

    /**
     * Construct an instance with the given keys.
     *
     * @param keys the index keys
     */
    public IndexModel(final Bson keys) {
        this(keys, new IndexOptions());
    }

    /**
     * Construct an instance with the given keys and options.
     *
     * @param keys the index keys
     * @param options the index options
     */
    public IndexModel(final Bson keys, final IndexOptions options) {
        this.keys = notNull("keys", keys);
        this.options = notNull("options", options);
    }

    /**
     * Gets the index keys.
     *
     * @return the index keys
     */
    public Bson getKeys() {
        return keys;
    }

    /**
     * Gets the index options.
     *
     * @return the index options
     */
    public IndexOptions getOptions() {
        return options;
    }
}

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
 * The default options for a collection to apply on the creation of indexes.
 *
 * @since 3.2
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 * @mongodb.driver.manual reference/command/createIndexes Index options
 * @mongodb.server.release 3.2
 */
public final class IndexOptionDefaults {
    private Bson storageEngine;

    /**
     * Gets the default storage engine options document for indexes.
     *
     * @return the storage engine options
     */
    @Nullable
    public Bson getStorageEngine() {
        return storageEngine;
    }

    /**
     * Sets the default storage engine options document for indexes.
     *
     * @param storageEngine the storage engine options
     * @return this
     */
    public IndexOptionDefaults storageEngine(@Nullable final Bson storageEngine) {
        this.storageEngine = storageEngine;
        return this;
    }

    @Override
    public String toString() {
        return "IndexOptionDefaults{"
                + "storageEngine=" + storageEngine
                + '}';
    }
}

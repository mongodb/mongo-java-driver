/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing a bulk write, which can include a combination of inserts, updates, and deletes.  By default the writes are ordered.
 *
 * @param <T>
 * @since 3.0
 */
public final class BulkWriteModel<T, D> {
    private final List<? extends WriteModel<? extends T, D>> operations;
    private boolean ordered;

    /**
     * Construct a new instance with the given list of write models.
     *
     * @param operations a non-null, non-empty list of write models
     */
    public BulkWriteModel(final List<? extends WriteModel<? extends T, D>> operations) {
        this.operations = notNull("operations", operations);
        isTrueArgument("operations list is not empty", operations.isEmpty());
    }

    /**
     * Gets the write models.
     *
     * @return a non-null, non-empty list of write models
     */
    public List<? extends WriteModel<? extends T, D>> getOperations() {
        return operations;
    }

    /**
     * If true, then when a write fails, return without performing the remaining
     * writes. If false, then when a write fails, continue with the remaining writes, if any.
     * Defaults to true.
     *
     * @return true if the writes are ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * If true, then when a write fails, return without performing the remaining
     * writes. If false, then when a write fails, continue with the remaining writes, if any.
     * Defaults to true.
     *
     * @param ordered true if the writes should be ordered
     * @return this
     */
    public BulkWriteModel<T, D> ordered(final boolean ordered) {
        this.ordered = ordered;
        return this;
    }
}

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

/**
 * The options to apply to a parallel scan operation.
 *
 * @mongodb.driver.manual reference/command/parallelCollectionScan/ parallelCollectionScan
 * @mongodb.server.release 2.6
 * @since 3.0
 * @deprecated this is an unused class and there should be no reason to use it
 */
@Deprecated
public class ParallelCollectionScanOptions {
    private int batchSize;

    /**
     * Gets the batch size to use for each cursor.
     *
     * @return the batch size to use for each cursor.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * The batch size to use for each cursor.
     *
     * @param batchSize the batch size, which must be greater than or equal to  0
     * @return this
     * @mongodb.driver.manual core/cursors/#cursor-batches BatchSize
     */
    public ParallelCollectionScanOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
}

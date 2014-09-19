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

/**
 * Return a list of cursors over the collection that can be used to scan it in parallel.
 *
 * <p> Note: As of MongoDB 2.6, this operation will work against a mongod, but not a mongos. </p>
 *
 * @mongodb.driver.manual manual/reference/command/parallelCollectionScan/ parallelCollectionScan
 * @mongodb.server.release 2.6
 * @since 3.0
 */
public class ParallelCollectionScanModel {
    private final int numCursors;
    private final ParallelCollectionScanOptions options;

    /**
     * Constructs an new instance
     *
     * @param numCursors the number of cursors requested to iterate in parallel.
     *                   This is an upper bound and the server may provide fewer.
     */
    public ParallelCollectionScanModel(final int numCursors) {
        this(numCursors, new ParallelCollectionScanOptions());
    }

    /**
     * Constructs an new instance
     *
     * @param numCursors the number of cursors requested to iterate in parallel.
     *                   This is an upper bound and the server may provide fewer.
     * @param options the options to apply to a parallel scan operation.
     */
    public ParallelCollectionScanModel(final int numCursors, final ParallelCollectionScanOptions options) {
        this.numCursors = numCursors;
        this.options = options;
    }

    /**
     * Gets the number of cursors requested to iterate in parallel. This is an upper bound and the server may provide fewer.
     *
     * @return the number of cursors requested to iterate in parallel.
     */
    public int getNumCursors() {
        return numCursors;
    }

    /**
     * Gets the options to apply to a parallel scan operation.
     *
     * @return the options to apply to a parallel scan operation.
     */
    public ParallelCollectionScanOptions getOptions() {
        return options;
    }

}

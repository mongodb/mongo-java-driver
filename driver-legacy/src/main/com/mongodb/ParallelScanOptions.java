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

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to use for a parallel collection scan.
 *
 * @mongodb.driver.manual reference/command/parallelCollectionScan/ Parallel Collection Scan
 * @since 2.12
 */
@Immutable
public final class ParallelScanOptions {
    private final int numCursors;
    private final int batchSize;
    private final ReadPreference readPreference;

    /**
     * Create a builder for the options
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for the options
     */
    @NotThreadSafe
    public static class Builder {
        private int numCursors = 1;
        private int batchSize;
        private ReadPreference readPreference;

        /**
         * Set the requested number of cursors to iterate in parallel.  This is an upper bound and the server may provide fewer.
         *
         * @param numCursors the number of cursors requested, which must be &gt;= 1
         * @return this
         */
        public Builder numCursors(final int numCursors) {
            isTrue("numCursors >= 1", numCursors >= 1);

            this.numCursors = numCursors;
            return this;
        }

        /**
         * The batch size to use for each cursor.
         *
         * @param batchSize the batch size, which must be &gt;= 0
         * @return this
         */
        public Builder batchSize(final int batchSize) {
            isTrue("batchSize >= 0", batchSize >= 0);
            this.batchSize = batchSize;
            return this;
        }

        /**
         * The read preference to use.
         *
         * @param readPreference the read preference
         * @return this
         */
        public Builder readPreference(final ReadPreference readPreference) {
            this.readPreference = notNull("readPreference", readPreference);
            return this;
        }

        /**
         * Creates a ParallelScanOptions with the settings initialised in this builder.
         *
         * @return a new ParallelScanOptions.
         */
        public ParallelScanOptions build() {
            return new ParallelScanOptions(this);
        }
    }

    /**
     * Gets the number of cursors requested.  The default value is 1.
     *
     * @return number of cursors requested.
     */
    public int getNumCursors() {
        return numCursors;
    }

    /**
     * Gets the batch size to use for each cursor.  The default value is 0, which tells the server to use its own default batch size.
     *
     * @return batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Gets the read preference to use.
     *
     * @return read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    private ParallelScanOptions(final Builder builder) {
        numCursors = builder.numCursors;
        batchSize = builder.batchSize;
        readPreference = builder.readPreference;
    }

}

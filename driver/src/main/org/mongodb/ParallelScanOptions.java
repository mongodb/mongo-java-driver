package org.mongodb;

import org.mongodb.annotations.Immutable;

import static org.mongodb.assertions.Assertions.isTrue;

/**
 * The options to use for a parallel collection scan.
 *
 * @since 3.0
 */
@Immutable
public final class ParallelScanOptions {
    private final int numCursors;
    private final int batchSize;

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
    public static class Builder {
        private int numCursors = 1;
        private int batchSize;

        /**
         * Set the requested number of cursors to iterate in parallel.  This is an upper bound and the server may provide fewer.
         *
         * @param numCursors the number of cursors requested, which must be >= 1
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
         * @param batchSize the batch size, which must be >= 0
         * @return this
         */
        public Builder batchSize(final int batchSize) {
            isTrue("batchSize >= 0", batchSize >= 0);
            this.batchSize = batchSize;
            return this;
        }

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

    private ParallelScanOptions(final Builder builder) {
        numCursors = builder.numCursors;
        batchSize = builder.batchSize;
    }
}

/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Options to apply to transactions. The default values for the options depend on context.  For options specified per-transaction, the
 * default values come from the default transaction options.  For the default transaction options themselves, the default values come from
 * the MongoClient on which the session was started.
 *
 * @see com.mongodb.session.ClientSession
 * @see ClientSessionOptions
 * @since 3.8
 * @mongodb.server.release 4.0
 */
@Immutable
public final class TransactionOptions {
    private final ReadConcern readConcern;
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private final Long maxCommitTimeMS;

    /**
     * Gets the read concern.
     *
     * @return the read concern
     */
    @Nullable
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    @Nullable
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the read preference.
     *
     * @return the write concern
     */
    @Nullable
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the maximum amount of time to allow a single commitTransaction command to execute.  The default is null, which places no
     * limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.server.release 4.2
     * @since 3.11
     */
    @Nullable
    public Long getMaxCommitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (maxCommitTimeMS == null) {
            return null;
        }
        return timeUnit.convert(maxCommitTimeMS, MILLISECONDS);
    }

    /**
     * Gets an instance of a builder
     *
     * @return a builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Merge the two provided transaction options, with the first taking precedence over the second.
     *
     * @param options        the transaction options, which take precedence for any property that is non-null
     * @param defaultOptions the default transaction options
     * @return the merged transaction options
     */
    public static TransactionOptions merge(final TransactionOptions options, final TransactionOptions defaultOptions) {
        notNull("options", options);
        notNull("defaultOptions", defaultOptions);
        return TransactionOptions.builder()
                .writeConcern(options.getWriteConcern() == null
                        ? defaultOptions.getWriteConcern() : options.getWriteConcern())
                .readConcern(options.getReadConcern() == null
                        ? defaultOptions.getReadConcern() : options.getReadConcern())
                .readPreference(options.getReadPreference() == null
                        ? defaultOptions.getReadPreference() : options.getReadPreference())
                .maxCommitTime(options.getMaxCommitTime(MILLISECONDS) == null
                                ? defaultOptions.getMaxCommitTime(MILLISECONDS) : options.getMaxCommitTime(MILLISECONDS),
                        MILLISECONDS)
                .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TransactionOptions that = (TransactionOptions) o;

        if (maxCommitTimeMS != null ? !maxCommitTimeMS.equals(that.maxCommitTimeMS) : that.maxCommitTimeMS != null) {
            return false;
        }
        if (readConcern != null ? !readConcern.equals(that.readConcern) : that.readConcern != null) {
            return false;
        }
        if (writeConcern != null ? !writeConcern.equals(that.writeConcern) : that.writeConcern != null) {
            return false;
        }
        if (readPreference != null ? !readPreference.equals(that.readPreference) : that.readPreference != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = readConcern != null ? readConcern.hashCode() : 0;
        result = 31 * result + (writeConcern != null ? writeConcern.hashCode() : 0);
        result = 31 * result + (readPreference != null ? readPreference.hashCode() : 0);
        result = 31 * result + (maxCommitTimeMS != null ? maxCommitTimeMS.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TransactionOptions{"
                + "readConcern=" + readConcern
                + ", writeConcern=" + writeConcern
                + ", readPreference=" + readPreference
                + ", maxCommitTimeMS" + maxCommitTimeMS
                + '}';
    }

    /**
     * The builder for transaction options
     */
    public static final class Builder {
        private ReadConcern readConcern;
        private WriteConcern writeConcern;
        private ReadPreference readPreference;
        private Long maxCommitTimeMS;

        /**
         * Sets the read concern.
         *
         * @param readConcern the read concern
         * @return this
         */
        public Builder readConcern(@Nullable final ReadConcern readConcern) {
            this.readConcern = readConcern;
            return this;
        }

        /**
         * Sets the write concern.
         *
         * @param writeConcern the write concern, which must be acknowledged
         * @return this
         */
        public Builder writeConcern(@Nullable final WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        /**
         * Sets the read preference.
         *
         * @param readPreference the read preference, which currently must be primary. This restriction may be relaxed in future versions.
         * @return this
         */
        public Builder readPreference(@Nullable final ReadPreference readPreference) {
            this.readPreference = readPreference;
            return this;
        }

        /**
         * Sets the maximum execution time on the server for the commitTransaction operation.
         *
         * @param maxCommitTime the max commit time, which must be either null or greater than zero, in the given time unit
         * @param timeUnit      the time unit, which may not be null
         * @return this
         * @mongodb.server.release 4.2
         * @since 3.11
         */
        public Builder maxCommitTime(@Nullable final Long maxCommitTime, final TimeUnit timeUnit) {
            if (maxCommitTime == null) {
                this.maxCommitTimeMS = null;
            } else {
                notNull("timeUnit", timeUnit);
                isTrueArgument("maxCommitTime > 0", maxCommitTime > 0);
                this.maxCommitTimeMS = MILLISECONDS.convert(maxCommitTime, timeUnit);
            }
            return this;
        }

        /**
         * Build the transaction options instance.
         *
         * @return The {@code TransactionOptions}
         */
        public TransactionOptions build() {
            return new TransactionOptions(this);
        }

        private Builder() {
        }
    }


    private TransactionOptions(final Builder builder) {
        readConcern = builder.readConcern;
        writeConcern = builder.writeConcern;
        readPreference = builder.readPreference;
        maxCommitTimeMS = builder.maxCommitTimeMS;
    }
}

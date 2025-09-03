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

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutSettings.convertAndValidateTimeoutNullable;
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
    private final Long timeoutMS;

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
     * The time limit for the full execution of the transaction.
     *
     * <p>If set the following deprecated options will be ignored:
     * {@code waitQueueTimeoutMS}, {@code socketTimeoutMS}, {@code wTimeoutMS}, {@code maxTimeMS} and {@code maxCommitTimeMS}</p>
     *
     * <ul>
     *   <li>{@code null} means that the timeout mechanism for operations will defer to using
     *   {@link ClientSessionOptions#getDefaultTimeout(TimeUnit)} or {@link MongoClientSettings#getTimeout(TimeUnit)}
     *   </li>
     *   <li>{@code 0} means infinite timeout.</li>
     *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
     * </ul>
     *
     * @param timeUnit the time unit
     * @return the timeout in the given time unit
     * @since 5.2
     */
    @Nullable
    @Alpha(Reason.CLIENT)
    public Long getTimeout(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        if (timeoutMS == null) {
            return null;
        }
        return timeUnit.convert(timeoutMS, MILLISECONDS);
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
                .timeout(options.getTimeout(MILLISECONDS) == null
                                ? defaultOptions.getTimeout(MILLISECONDS) : options.getTimeout(MILLISECONDS),
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

        if (!Objects.equals(timeoutMS, that.timeoutMS)) {
            return false;
        }
        if (!Objects.equals(maxCommitTimeMS, that.maxCommitTimeMS)) {
            return false;
        }
        if (!Objects.equals(readConcern, that.readConcern)) {
            return false;
        }
        if (!Objects.equals(writeConcern, that.writeConcern)) {
            return false;
        }
        if (!Objects.equals(readPreference, that.readPreference)) {
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
        result = 31 * result + (timeoutMS != null ? timeoutMS.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TransactionOptions{"
                + "readConcern=" + readConcern
                + ", writeConcern=" + writeConcern
                + ", readPreference=" + readPreference
                + ", maxCommitTimeMS=" + maxCommitTimeMS
                + ", timeoutMS=" + timeoutMS
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
        @Nullable
        private Long timeoutMS;

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
         * Sets the time limit for the full execution of the operations for this transaction.
         *
         * <ul>
         *   <li>{@code null} means that the timeout mechanism for operations will defer to using:
         *    <ul>
         *        <li>{@code waitQueueTimeoutMS}: The maximum wait time in milliseconds that a thread may wait for a connection to become
         *        available</li>
         *        <li>{@code socketTimeoutMS}: How long a send or receive on a socket can take before timing out.</li>
         *        <li>{@code wTimeoutMS}: How long the server will wait for the write concern to be fulfilled before timing out.</li>
         *        <li>{@code maxTimeMS}: The cumulative time limit for processing operations on a cursor.
         *        See: <a href="https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS">cursor.maxTimeMS</a>.</li>
         *        <li>{@code maxCommitTimeMS}: The maximum amount of time to allow a single {@code commitTransaction} command to execute.</li>
         *   </ul>
         *   </li>
         *   <li>{@code 0} means infinite timeout.</li>
         *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
         * </ul>
         *
         * @param timeout the timeout
         * @param timeUnit the time unit
         * @return this
         * @since 5.2
         */
        @Alpha(Reason.CLIENT)
        public Builder timeout(@Nullable final Long timeout, final TimeUnit timeUnit) {
            this.timeoutMS = convertAndValidateTimeoutNullable(timeout, timeUnit);
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
        timeoutMS = builder.timeoutMS;
    }
}

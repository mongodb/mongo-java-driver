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

package com.mongodb;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutSettings.convertAndValidateTimeout;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to a {@code ClientSession}.
 *
 * @mongodb.server.release 3.6
 * @since 3.6
 * @see ClientSession
 * @mongodb.driver.dochub core/causal-consistency Causal Consistency
 */
@Immutable
public final class ClientSessionOptions {

    private final Boolean causallyConsistent;
    private final Boolean snapshot;
    private final Long defaultTimeoutMS;
    private final TransactionOptions defaultTransactionOptions;

    /**
     * Whether operations using the session should causally consistent with each other.
     *
     * @return whether operations using the session should be causally consistent.  A null value indicates to use the global default,
     * which is currently true.
     * @mongodb.driver.dochub core/causal-consistency Causal Consistency
     */
    @Nullable
    public Boolean isCausallyConsistent() {
        return causallyConsistent;
    }

    /**
     * Whether read operations using this session should all share the same snapshot.
     *
     * @return whether read operations using this session should all share the same snapshot. A null value indicates to use the global
     * default, which is false.
     * @since 4.3
     * @mongodb.server.release 5.0
     * @mongodb.driver.manual  reference/read-concern-snapshot/#read-concern-and-atclustertime Snapshot reads
     */
    @Nullable
    public Boolean isSnapshot() {
        return snapshot;
    }

    /**
     * Gets the default transaction options for the session.
     *
     * @return the default transaction options for the session
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    public TransactionOptions getDefaultTransactionOptions() {
        return defaultTransactionOptions;
    }

    /**
     * Gets the default time limit for the following operations executed on the session:
     *
     * <ul>
     *   <li>{@code commitTransaction}</li>
     *   <li>{@code abortTransaction}</li>
     *   <li>{@code withTransaction}</li>
     *   <li>{@code close}</li>
     * </ul>
     * @param timeUnit the time unit
     * @return the default timeout
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    @Nullable
    public Long getDefaultTimeout(final TimeUnit timeUnit) {
        return defaultTimeoutMS == null ? null : timeUnit.convert(defaultTimeoutMS, MILLISECONDS);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClientSessionOptions that = (ClientSessionOptions) o;
        return Objects.equals(causallyConsistent, that.causallyConsistent)
                && Objects.equals(snapshot, that.snapshot)
                && Objects.equals(defaultTimeoutMS, that.defaultTimeoutMS)
                && Objects.equals(defaultTransactionOptions, that.defaultTransactionOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(causallyConsistent, snapshot, defaultTimeoutMS, defaultTransactionOptions);
    }

    @Override
    public String toString() {
        return "ClientSessionOptions{"
                + "causallyConsistent=" + causallyConsistent
                + ", snapshot=" + snapshot
                + ", defaultTimeoutMS=" + defaultTimeoutMS
                + ", defaultTransactionOptions=" + defaultTransactionOptions
                + '}';
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
     * Gets an instance of a builder initialized with the given options
     *
     * @param options the options with which to initialize the builder
     * @return a builder instance
     * @since 3.8
     */
    public static Builder builder(final ClientSessionOptions options) {
        notNull("options", options);
        Builder builder = new Builder();
        builder.causallyConsistent = options.isCausallyConsistent();
        builder.snapshot = options.isSnapshot();
        builder.defaultTransactionOptions = options.getDefaultTransactionOptions();
        builder.defaultTimeoutMS = options.defaultTimeoutMS;
        return builder;
    }

    /**
     * A builder for instances of {@code ClientSession}
     */
    @NotThreadSafe
    public static final class Builder {
        private Boolean causallyConsistent;
        private Boolean snapshot;
        private Long defaultTimeoutMS;
        private TransactionOptions defaultTransactionOptions = TransactionOptions.builder().build();

        /**
         * Sets whether operations using the session should causally consistent with each other.
         *
         * @param causallyConsistent whether operations using the session should be causally consistent
         * @return this
         * @mongodb.driver.dochub core/causal-consistency Causal Consistency
         */
        public Builder causallyConsistent(final boolean causallyConsistent) {
            this.causallyConsistent = causallyConsistent;
            return this;
        }

        /**
         * Sets whether read operations using the session should share the same snapshot.
         *
         * <p>
         * The default value is unset, in which case the driver will use the global default value, which is currently false.
         * </p>
         *
         * @param snapshot true for snapshot reads, false otherwise
         * @return this
         * @since 4.3
         * @mongodb.server.release 5.0
         * @mongodb.driver.manual  reference/read-concern-snapshot/#read-concern-and-atclustertime Snapshot reads
         */
        public Builder snapshot(final boolean snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        /**
         * Sets whether operations using the session should causally consistent with each other.
         *
         * @param defaultTransactionOptions the default transaction options to use for all transactions on this session,
         * @return this
         * @since 3.8
         * @mongodb.server.release 4.0
         */
        public Builder defaultTransactionOptions(final TransactionOptions defaultTransactionOptions) {
            this.defaultTransactionOptions = notNull("defaultTransactionOptions", defaultTransactionOptions);
            return this;
        }

        /**
         * Sets the default time limit for the following operations executed on the session:
         *
         * <ul>
         *   <li>{@code commitTransaction}</li>
         *   <li>{@code abortTransaction}</li>
         *   <li>{@code withTransaction}</li>
         *   <li>{@code close}</li>
         * </ul>
         * @param defaultTimeout the timeout
         * @param timeUnit the time unit
         * @return this
         * @since 5.2
         * @see #getDefaultTimeout
         */
        @Alpha(Reason.CLIENT)
        public Builder defaultTimeout(final long defaultTimeout, final TimeUnit timeUnit) {
            this.defaultTimeoutMS = convertAndValidateTimeout(defaultTimeout, timeUnit, "defaultTimeout");
            return this;
        }

        /**
         * Build the session options instance.
         *
         * @return The {@code ClientSessionOptions}
         */
        public ClientSessionOptions build() {
            return new ClientSessionOptions(this);
        }

        private Builder() {
        }
    }

    private ClientSessionOptions(final Builder builder) {
        if (builder.causallyConsistent != null && builder.causallyConsistent && builder.snapshot != null && builder.snapshot) {
            throw new IllegalArgumentException("A session can not be both a snapshot and causally consistent");
        }
        this.causallyConsistent = builder.causallyConsistent != null || builder.snapshot == null
                ? builder.causallyConsistent
                : Boolean.valueOf(!builder.snapshot);
        this.snapshot = builder.snapshot;
        this.defaultTransactionOptions = builder.defaultTransactionOptions;
        this.defaultTimeoutMS = builder.defaultTimeoutMS;
    }
}

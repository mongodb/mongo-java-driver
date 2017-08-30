/*
 * Copyright 2017 MongoDB, Inc.
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
 *
 */

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * The options to apply to a {@code ClientSession}.
 *
 * @mongodb.server.release 3.6
 * @since 3.6
 * @see ClientSession
 * @see MongoClient#startSession(ClientSessionOptions)
 */
@Immutable
public final class ClientSessionOptions {

    private final Boolean causallyConsistent;
    private final BsonDocument initialClusterTime;
    private final BsonTimestamp initialOperationTime;

    /**
     * Whether operations using the session should causally consistent with each other.
     *
     * @return whether operations using the session should be causally consistent.  A null value indicates to use the the global default,
     * which is currently true.
     */
    public Boolean isCausallyConsistent() {
        return causallyConsistent;
    }

    /**
     * Gets the initial cluster time to apply to the client session
     *
     * @return the initial cluster time, which may be null
     */
    public BsonDocument getInitialClusterTime() {
        return initialClusterTime;
    }

    /**
     * Gets the initial operation time to apply to the client session
     * @return the initial operation time, which may be null
     */
    public BsonTimestamp getInitialOperationTime() {
        return initialOperationTime;
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
     * A builder for instances of {@code ClientSession}
     */
    @NotThreadSafe
    public static final class Builder {
        private Boolean causallyConsistent;
        private BsonDocument initialClusterTime;
        private BsonTimestamp initialOperationTime;

        /**
         * Sets whether operations using the session should causally consistent with each other.
         *
         * @param causallyConsistent whether operations using the session should be causally consistent
         * @return this
         */
        public Builder causallyConsistent(final boolean causallyConsistent) {
            this.causallyConsistent = causallyConsistent;
            return this;
        }

        /**
         * Sets the initial cluster time to apply to the client session
         *
         * @param initialClusterTime the initial cluster time, which may be null
         * @return this
         */
        public Builder initialClusterTime(final BsonDocument initialClusterTime) {
            this.initialClusterTime = initialClusterTime;
            return this;
        }

        /**
         * Gets the initial operation time to apply to the client session
         *
         * @param initialOperationTime the initial operation time, which may be null
         * @return this
         */
        public Builder initialOperationTime(final BsonTimestamp initialOperationTime) {
            this.initialOperationTime = initialOperationTime;
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
        this.causallyConsistent = builder.causallyConsistent;
        this.initialClusterTime = builder.initialClusterTime;
        this.initialOperationTime = builder.initialOperationTime;
    }
}

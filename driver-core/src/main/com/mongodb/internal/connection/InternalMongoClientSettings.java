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
package com.mongodb.internal.connection;

import com.mongodb.annotations.Immutable;

import java.util.Objects;

/**
 * Internal settings for MongoClient that are not part of the public API.
 * Used for testing and internal configuration purposes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@Immutable
public final class InternalMongoClientSettings {

    private static final InternalMongoClientSettings DEFAULTS = builder().build();

    private final InternalConnectionPoolSettings internalConnectionPoolSettings;
    private final boolean recordEverything;

    private InternalMongoClientSettings(final Builder builder) {
        this.internalConnectionPoolSettings = builder.internalConnectionPoolSettings != null
                ? builder.internalConnectionPoolSettings
                : InternalConnectionPoolSettings.builder().build();
        this.recordEverything = builder.recordEverything;
    }

    /**
     * Gets the default internal settings for production use.
     *
     * @return the default settings
     */
    public static InternalMongoClientSettings getDefaults() {
        return DEFAULTS;
    }

    /**
     * Creates a new builder for InternalMongoClientSettings.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the internal connection pool settings.
     *
     * @return the internal connection pool settings
     */
    public InternalConnectionPoolSettings getInternalConnectionPoolSettings() {
        return internalConnectionPoolSettings;
    }

    /**
     * Indicates whether to record all commands including security-sensitive commands
     * (like authentication commands) to the command listener.
     * <p>
     * When disabled (the default for production), security-sensitive commands are not recorded
     * to the command listener for security reasons. When enabled, all commands are recorded,
     * which is useful for testing authentication and handshake behavior.
     * </p>
     *
     * @return true if all commands should be recorded
     */
    public boolean isRecordEverything() {
        return recordEverything;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalMongoClientSettings that = (InternalMongoClientSettings) o;
        return recordEverything == that.recordEverything
                && Objects.equals(internalConnectionPoolSettings, that.internalConnectionPoolSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(internalConnectionPoolSettings, recordEverything);
    }

    @Override
    public String toString() {
        return "InternalMongoClientSettings{"
                + "internalConnectionPoolSettings=" + internalConnectionPoolSettings
                + ", recordEverything=" + recordEverything
                + '}';
    }

    /**
     * A builder for InternalMongoClientSettings.
     */
    public static final class Builder {
        private InternalConnectionPoolSettings internalConnectionPoolSettings = InternalConnectionPoolSettings.builder().build();
        private boolean recordEverything = false;

        private Builder() {
        }

        /**
         * Sets the internal connection pool settings.
         *
         * @param internalConnectionPoolSettings the internal connection pool settings
         * @return this
         */
        public Builder internalConnectionPoolSettings(
                final InternalConnectionPoolSettings internalConnectionPoolSettings) {
            this.internalConnectionPoolSettings = internalConnectionPoolSettings;
            return this;
        }

        /**
         * Sets whether to record all commands including security-sensitive commands.
         * <p>
         * Default is {@code false} for production use. Set to {@code true} for testing
         * authentication and handshake behavior.
         * </p>
         *
         * @param recordEverything whether to record all commands
         * @return this
         */
        public Builder recordEverything(final boolean recordEverything) {
            this.recordEverything = recordEverything;
            return this;
        }

        /**
         * Builds the InternalMongoClientSettings.
         *
         * @return the internal settings
         */
        public InternalMongoClientSettings build() {
            return new InternalMongoClientSettings(this);
        }
    }
}


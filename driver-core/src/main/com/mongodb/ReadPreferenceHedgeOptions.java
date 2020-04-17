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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;

/**
 * Options to apply to hedged reads in the server.
 *
 * @since 4.1
 * @mongodb.server.release 4.4
 */
@Immutable
public final class ReadPreferenceHedgeOptions {
    private final boolean enabled;

    /**
     * Gets whether hedged reads are enabled in the server.
     *
     * @return true if hedged reads are enabled in the server
     */
    public boolean isEnabled() {
        return enabled;
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
     * Convert the hedge options to a BsonDocument.
     *
     * @return a BsonDocument containing the hedge options
     */
    public BsonDocument toBsonDocument() {
        return new BsonDocument("enabled", new BsonBoolean(enabled));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReadPreferenceHedgeOptions that = (ReadPreferenceHedgeOptions) o;

        return enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return enabled ? 1 : 0;
    }

    @Override
    public String toString() {
        return "ReadPreferenceHedgeOptions{"
                + "enabled=" + enabled
                + '}';
    }

    /**
     * The builder for read preference hedge options
     */
    public static final class Builder {
        private boolean enabled;

        /**
         * Sets whether hedged reads are enabled in the server.
         *
         * @param enabled true if hedged reads are enabled
         * @return this
         */
        public Builder enabled(final boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Build the transaction options instance.
         *
         * @return The {@code TransactionOptions}
         */
        public ReadPreferenceHedgeOptions build() {
            return new ReadPreferenceHedgeOptions(this);
        }

        private Builder() {
        }
    }


    private ReadPreferenceHedgeOptions(final Builder builder) {
        enabled = builder.enabled;
    }
}

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

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.annotations.Immutable;

/**
 * Settings for connecting to MongoDB via SSL.
 *
 * @since 3.0
 */
@Immutable
public class SslSettings {
    private final boolean enabled;

    /**
     * Gets a Builder for creating a new SSLSettings instance.
     *
     * @return a new Builder for SSLSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for creating SSLSettings.
     */
    public static class Builder {
        private boolean enabled;

        /**
         * Define whether SSL should be enabled.
         *
         * @param enabled should be true if SSL is to be enabled.
         * @return this
         */
        public Builder enabled(final boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Take the settings from the given ConnectionString and set them in this builder.
         *
         * @param connectionString a URI with details on how to connect to MongoDB.
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            if (connectionString.getSslEnabled() != null) {
                this.enabled = connectionString.getSslEnabled();
            }
            return this;
        }

        /**
         * Create a new SSLSettings from the settings in this builder.
         *
         * @return a new SSL settings
         */
        public SslSettings build() {
            return new SslSettings(this);
        }
    }

    /**
     * Returns whether SSL is enabled.
     *
     * @return true if SSL is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    SslSettings(final Builder builder) {
        enabled = builder.enabled;
    }

    @Override
    public String toString() {
        return "SSLSettings{"
               + "enabled=" + enabled
               + '}';
    }
}

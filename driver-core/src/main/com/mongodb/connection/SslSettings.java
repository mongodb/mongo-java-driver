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
import com.mongodb.MongoInternalException;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;

/**
 * Settings for connecting to MongoDB via SSL.
 *
 * @since 3.0
 */
@Immutable
public class SslSettings {
    private final boolean enabled;
    private final boolean invalidHostNameAllowed;

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
    @NotThreadSafe
    public static class Builder {
        private boolean enabled;
        private boolean invalidHostNameAllowed;

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
         * Define whether invalid host names should be allowed.  Defaults to false.  Take care before setting this to true, as it makes
         * the application susceptible to man-in-the-middle attacks.
         *
         * @param invalidHostNameAllowed whether invalid host names are allowed.
         * @return this
         */
        public Builder invalidHostNameAllowed(final boolean invalidHostNameAllowed) {
            this.invalidHostNameAllowed = invalidHostNameAllowed;
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
            if (connectionString.getSslInvalidHostnameAllowed() != null) {
                this.invalidHostNameAllowed = connectionString.getSslInvalidHostnameAllowed();
            }
            return this;
        }

        /**
         * Create a new SSLSettings from the settings in this builder.
         *
         * @return a new SSL settings
         * @throws com.mongodb.MongoInternalException if enabled is true, invalidHostNameAllowed is false, and the {@code "java.version"}
         * system property starts with 1.6
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

    /**
     * Returns whether invalid host names should be allowed.  Defaults to false.  Take care before setting this to true, as it makes
     * the application susceptible to man-in-the-middle attacks.
     *
     * @return true if invalid host names are allowed.
     */
    public boolean isInvalidHostNameAllowed() {
        return invalidHostNameAllowed;
    }

    SslSettings(final Builder builder) {
        enabled = builder.enabled;
        invalidHostNameAllowed = builder.invalidHostNameAllowed;
        if (enabled && !invalidHostNameAllowed) {
            if (System.getProperty("java.version").startsWith("1.6.")) {
                throw new MongoInternalException("By default, SSL connections are only supported on Java 7 or later.  If the application "
                                                 + "must run on Java 6, you must set the SslSettings.invalidHostNameAllowed property to "
                                                 + "false");
            }
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SslSettings that = (SslSettings) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (invalidHostNameAllowed != that.invalidHostNameAllowed) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (invalidHostNameAllowed ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SslSettings{"
               + "enabled=" + enabled
               + ", invalidHostNameAllowed=" + invalidHostNameAllowed
               + '}';
    }
}

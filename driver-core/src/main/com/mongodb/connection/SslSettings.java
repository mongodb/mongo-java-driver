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

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;

import javax.net.ssl.SSLContext;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Settings for connecting to MongoDB via SSL.
 *
 * @since 3.0
 */
@Immutable
public class SslSettings {
    private final boolean enabled;
    private final boolean invalidHostNameAllowed;
    private final SSLContext context;

    /**
     * Gets a Builder for creating a new SSLSettings instance.
     *
     * @return a new Builder for SSLSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder instance.
     *
     * @param sslSettings existing SslSettings to default the builder settings on.
     * @return a builder
     * @since 3.7
     */
    public static Builder builder(final SslSettings sslSettings) {
        return builder().applySettings(sslSettings);
    }

    /**
     * A builder for creating SSLSettings.
     */
    @NotThreadSafe
    public static final class Builder {
        private boolean enabled;
        private boolean invalidHostNameAllowed;
        private SSLContext context;

        private Builder(){
        }

        /**
         * Applies the sslSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param sslSettings the sslSettings
         * @return this
         * @since 3.7
         */
        public Builder applySettings(final SslSettings sslSettings) {
            notNull("sslSettings", sslSettings);
            enabled = sslSettings.enabled;
            invalidHostNameAllowed = sslSettings.invalidHostNameAllowed;
            context = sslSettings.context;
            return this;
        }

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
         * Sets the SSLContext for use when SSL is enabled.
         *
         * @param context the SSLContext to use for connections.  Ignored if SSL is not enabled.
         * @return this
         * @since 3.5
         */
        public Builder context(final SSLContext context) {
            this.context = context;
            return this;
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            Boolean sslEnabled = connectionString.getSslEnabled();
            if (sslEnabled != null) {
                this.enabled = sslEnabled;
            }

            Boolean sslInvalidHostnameAllowed = connectionString.getSslInvalidHostnameAllowed();
            if (sslInvalidHostnameAllowed != null) {
                this.invalidHostNameAllowed = sslInvalidHostnameAllowed;
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

    /**
     * Gets the SSLContext configured for use with SSL connections.
     *
     * @return the SSLContext, which defaults to null if not configured.  In that case {@code SSLContext.getDefault()} will be used if SSL
     * is enabled.
     * @since 3.5
     * @see SSLContext#getDefault()
     */
    public SSLContext getContext() {
        return context;
    }

    SslSettings(final Builder builder) {
        enabled = builder.enabled;
        invalidHostNameAllowed = builder.invalidHostNameAllowed;
        context = builder.context;
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
        return context != null ? context.equals(that.context) : that.context == null;
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (invalidHostNameAllowed ? 1 : 0);
        result = 31 * result + (context != null ? context.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SslSettings{"
               + "enabled=" + enabled
               + ", invalidHostNameAllowed=" + invalidHostNameAllowed
               + ", context=" + context
               + '}';
    }
}

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


import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Settings for connecting to MongoDB via a SOCKS5 proxy server.
 *
 * @since 4.11
 */
@Immutable
public class ProxySettings {

    /**
     * The proxy host.
     */
    @Nullable
    private final String host;
    /**
     * The proxy port.
     */
    @Nullable
    private final Integer port;

    @Nullable
    private final String username;
    @Nullable
    private final String password;

    /**
     * Creates a {@link Builder} for creating a new {@link ProxySettings} instance.
     *
     * @return a new {@link Builder} for {@link ProxySettings}.
     * @since 4.11
     */
    public static ProxySettings.Builder builder() {
        return new ProxySettings.Builder();
    }

    /**
     * Creates a {@link Builder} for creating a new {@link ProxySettings} instance.
     *
     * @param proxySettings existing {@link ProxySettings} to default the builder settings on.
     * @return a new {@link Builder} for {@link ProxySettings}.
     * @since 4.11
     */
    public static ProxySettings.Builder builder(final ProxySettings proxySettings) {
        return builder().applySettings(proxySettings);
    }

    /**
     * A builder for an instance of {@code ProxySettings}.
     */
    public static final class Builder {
        private String host;
        private Integer port;
        private String username;
        private String password;

        private Builder() {
        }

        public ProxySettings.Builder applySettings(final ProxySettings proxySettings) {
            notNull("ProxySettings", proxySettings);
            this.host = proxySettings.host;
            this.port = proxySettings.port;
            this.username = proxySettings.username;
            this.password = proxySettings.password;
            return this;
        }


        public ProxySettings.Builder host(final String host) {
            notNull("proxyHost", host);
            isTrue("proxyHost is not empty", host.trim().length() > 0);
            this.host = host;
            return this;
        }

        public ProxySettings.Builder port(final int port) {
            isTrue("proxyPort is equal or greater than 0", port >= 0);
            this.port = port;
            return this;
        }

        public ProxySettings.Builder username(final String username) {
            notNull("username", username);
            isTrue("username is not empty", !username.isEmpty());
            this.username = username;
            return this;
        }

        public ProxySettings.Builder password(final String password) {
            notNull("password", password);
            isTrue("password is not empty", !password.isEmpty());
            this.password = password;
            return this;
        }


        /**
         * Takes the proxy settings from the given {@code ConnectionString} and applies them to the {@link Builder}.
         *
         * @param connectionString the connection string containing details of how to connect to proxy server.
         * @return this.
         * @see ConnectionString#getProxyHost()
         * @see ConnectionString#getProxyPort()
         * @see ConnectionString#getProxyUsername()
         * @see ConnectionString#getProxyPassword()
         */
        public ProxySettings.Builder applyConnectionString(final ConnectionString connectionString) {
            String proxyHost = connectionString.getProxyHost();
            if (proxyHost != null) {
                this.host(proxyHost);
            }

            Integer proxyPort = connectionString.getProxyPort();
            if (proxyPort != null) {
                this.port(proxyPort);
            }

            String proxyUsername = connectionString.getProxyUsername();
            if (proxyUsername != null) {
                this.username(proxyUsername);
            }

            String proxyPassword = connectionString.getProxyPassword();
            if (proxyPassword != null) {
                this.password(proxyPassword);
            }

            return this;
        }

        /**
         * Build an instance of {@code ProxySettings}.
         *
         * @return the {@link ProxySettings}.
         */
        public ProxySettings build() {
            return new ProxySettings(this);
        }
    }

    @Nullable
    public String getHost() {
        return host;
    }

    @Nullable
    public Integer getPort() {
        return port;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProxySettings)) {
            return false;
        }

        ProxySettings that = (ProxySettings) o;

        if (port != that.port) {
            return false;
        }
        if (!Objects.equals(host, that.host)) {
            return false;
        }
        if (!Objects.equals(username, that.username)) {
            return false;
        }
        return Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ProxySettings{"
                + "host=" + host
                + ", port=" + port
                + ", username=" + username
                + ", password=" + password
                + '}';
    }

    private ProxySettings(final ProxySettings.Builder builder) {
        if (builder.host == null) {
            isTrue("proxyPassword can only be specified with proxyHost",
                    builder.password == null);
            isTrue("proxyUsername can only be specified with proxyHost",
                    builder.username == null);
            isTrue("proxyPort can only be specified with proxyHost",
                    builder.port == null);
        }
        isTrue("Both proxyUsername and proxyPassword must be set together. They cannot be set individually",
                (builder.username == null) == (builder.password == null));

        this.host = builder.host;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
    }
}


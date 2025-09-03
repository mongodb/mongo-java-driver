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


import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * This setting is only applicable when communicating with a MongoDB server using the synchronous variant of {@code MongoClient}.
 * <p>
 * This setting is furthermore ignored if:
 * <ul>
 *     <li>the communication is via {@linkplain com.mongodb.UnixServerAddress Unix domain socket}.</li>
 *     <li>a {@link TransportSettings} is {@linkplain MongoClientSettings.Builder#transportSettings(TransportSettings)}
 *     configured}.</li>
 * </ul>
 *
 * @see SocketSettings#getProxySettings()
 * @see ClientEncryptionSettings#getKeyVaultMongoClientSettings()
 * @see AutoEncryptionSettings#getKeyVaultMongoClientSettings()
 * @since 4.11
 */
@Immutable
public final class ProxySettings {

    private static final int DEFAULT_PORT = 1080;
    @Nullable
    private final String host;

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
     */
    public static ProxySettings.Builder builder() {
        return new ProxySettings.Builder();
    }

    /**
     * Creates a {@link Builder} for creating a new {@link ProxySettings} instance.
     *
     * @param proxySettings existing {@link ProxySettings} to default the builder settings on.
     * @return a new {@link Builder} for {@link ProxySettings}.
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

        /**
         * Applies the provided {@link ProxySettings} to this builder instance.
         *
         * <p>
         * Note: This method overwrites all existing proxy settings previously configured in this builder.
         *
         * @param proxySettings The {@link ProxySettings} instance containing the proxy configuration to apply.
         * @return This {@link ProxySettings.Builder} instance with the updated proxy settings applied.
         * @throws IllegalArgumentException If the provided {@link ProxySettings} instance is null.
         */
        public ProxySettings.Builder applySettings(final ProxySettings proxySettings) {
            notNull("ProxySettings", proxySettings);
            this.host = proxySettings.host;
            this.port = proxySettings.port;
            this.username = proxySettings.username;
            this.password = proxySettings.password;
            return this;
        }

        /**
         * Sets the SOCKS5 proxy host to establish a connection through.
         *
         * <p>The host can be specified as an IPv4 address (e.g., "192.168.1.1"),
         * an IPv6 address (e.g., "2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
         * or a domain name (e.g., "proxy.example.com"). </p>
         *
         * @param host The SOCKS5 proxy host to set.
         * @return This ProxySettings.Builder instance, configured with the specified proxy host.
         * @throws IllegalArgumentException If the provided host is null or empty after trimming.
         * @see ProxySettings.Builder#port(int)
         * @see #getHost()
         */
        public ProxySettings.Builder host(final String host) {
            notNull("proxyHost", host);
            isTrueArgument("proxyHost is not empty", host.trim().length() > 0);
            this.host = host;
            return this;
        }

        /**
         * Sets the port number for the SOCKS5 proxy server. The port should be a non-negative integer
         * representing the port through which the SOCKS5 proxy connection will be established.
         * <p>
         * If a port is specified via this method, a corresponding host must be provided using the {@link #host(String)} method.
         * <p>
         * If no port is provided, the default port 1080 will be used.
         *
         * @param port The port number to set for the SOCKS5 proxy server.
         * @return This ProxySettings.Builder instance, configured with the specified proxy port.
         * @throws IllegalArgumentException If the provided port is negative.
         * @see ProxySettings.Builder#host(String)
         * @see #getPort()
         */
        public ProxySettings.Builder port(final int port) {
            isTrueArgument("proxyPort is within the valid range (0 to 65535)", port >= 0 && port <= 65535);
            this.port = port;
            return this;
        }

        /**
         * Sets the username for authenticating with the SOCKS5 proxy server.
         * The provided username should not be empty or null.
         * <p>
         * If a username is specified, the corresponding password and proxy host must also be specified using the
         * {@link #password(String)} and {@link #host(String)} methods, respectively.
         *
         * @param username The username to set for proxy authentication.
         * @return This ProxySettings.Builder instance, configured with the specified username.
         * @throws IllegalArgumentException If the provided username is empty or null.
         * @see ProxySettings.Builder#password(String)
         * @see ProxySettings.Builder#host(String)
         * @see #getUsername()
         */
        public ProxySettings.Builder username(final String username) {
            notNull("username", username);
            isTrueArgument("username is not empty", !username.isEmpty());
            isTrueArgument("username's length in bytes is not greater than 255",
                    username.getBytes(StandardCharsets.UTF_8).length <= 255);
            this.username = username;
            return this;
        }

        /**
         * Sets the password for authenticating with the SOCKS5 proxy server.
         * The provided password should not be empty or null.
         * <p>
         * If a password is specified, the corresponding username and proxy host must also be specified using the
         * {@link #username(String)} and {@link #host(String)} methods, respectively.
         *
         * @param password The password to set for proxy authentication.
         * @return This ProxySettings.Builder instance, configured with the specified password.
         * @throws IllegalArgumentException If the provided password is empty or null.
         * @see ProxySettings.Builder#username(String)
         * @see ProxySettings.Builder#host(String)
         * @see #getPassword()
         */
        public ProxySettings.Builder password(final String password) {
            notNull("password", password);
            isTrueArgument("password is not empty", !password.isEmpty());
            isTrueArgument("password's length in bytes is not greater than 255",
                    password.getBytes(StandardCharsets.UTF_8).length <= 255);
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

    /**
     * Gets the SOCKS5 proxy host.
     *
     * @return the proxy host value. {@code null} if and only if the {@linkplain #isProxyEnabled() proxy functionality is not enabled}.
     * @see Builder#host(String)
     */
    @Nullable
    public String getHost() {
        return host;
    }

    /**
     * Gets the SOCKS5 proxy port.
     *
     * @return The port number of the SOCKS5 proxy. If a custom port has been set using {@link Builder#port(int)},
     * that custom port value is returned. Otherwise, the default SOCKS5 port {@value #DEFAULT_PORT} is returned.
     * @see Builder#port(int)
     */
    public int getPort() {
        if (port != null) {
            return port;
        }
        return DEFAULT_PORT;
    }

    /**
     * Gets the SOCKS5 proxy username.
     *
     * @return the proxy username value.
     * @see Builder#username(String)
     */
    @Nullable
    public String getUsername() {
        return username;
    }

    /**
     * Gets the SOCKS5 proxy password.
     *
     * @return the proxy password value.
     * @see Builder#password(String)
     */
    @Nullable
    public String getPassword() {
        return password;
    }

    /**
     * Checks if the SOCKS5 proxy is enabled.
     *
     * @return {@code true} if the proxy is enabled, {@code false} otherwise.
     * @see Builder#host(String)
     */
    public boolean isProxyEnabled() {
        return host != null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ProxySettings that = (ProxySettings) o;
        return Objects.equals(host, that.host)
                && Objects.equals(port, that.port)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, username, password);
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
            isTrue("proxyPort can only be specified with proxyHost",
                    builder.port == null);
            isTrue("proxyPassword can only be specified with proxyHost",
                    builder.password == null);
            isTrue("proxyUsername can only be specified with proxyHost",
                    builder.username == null);
        }
        isTrue("Both proxyUsername and proxyPassword must be set together. They cannot be set individually",
                (builder.username == null) == (builder.password == null));

        this.host = builder.host;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
    }
}


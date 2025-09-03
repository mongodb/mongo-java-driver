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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Evolving;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.AuthenticationMechanism.MONGODB_AWS;
import static com.mongodb.AuthenticationMechanism.MONGODB_OIDC;
import static com.mongodb.AuthenticationMechanism.MONGODB_X509;
import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.OidcAuthenticator.OidcValidator.validateCreateOidcCredential;
import static com.mongodb.internal.connection.OidcAuthenticator.OidcValidator.validateOidcCredentialConstruction;

/**
 * Represents credentials to authenticate to a mongo server,as well as the source of the credentials and the authentication mechanism to
 * use.
 *
 * @since 2.11
 */
@Immutable
public final class MongoCredential {

    private final AuthenticationMechanism mechanism;
    private final String userName;
    private final String source;
    private final char[] password;
    private final Map<String, Object> mechanismProperties;

    /**
     * The GSSAPI mechanism.  See the <a href="http://tools.ietf.org/html/rfc4752">RFC</a>.
     *
     * @mongodb.driver.manual core/authentication/#kerberos-authentication GSSAPI
     */
    public static final String GSSAPI_MECHANISM = GSSAPI.getMechanismName();

    /**
     * The PLAIN mechanism.  See the <a href="http://www.ietf.org/rfc/rfc4616.txt">RFC</a>.
     *
     * @since 2.12
     * @mongodb.driver.manual core/authentication/#ldap-proxy-authority-authentication PLAIN
     */
    public static final String PLAIN_MECHANISM = PLAIN.getMechanismName();

    /**
     * The MongoDB X.509
     *
     * @since 2.12
     * @mongodb.driver.manual core/authentication/#x-509-certificate-authentication X-509
     */
    public static final String MONGODB_X509_MECHANISM = MONGODB_X509.getMechanismName();

    /**
     * The SCRAM-SHA-1 Mechanism.
     *
     * @since 2.13
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1
     */
    public static final String SCRAM_SHA_1_MECHANISM = SCRAM_SHA_1.getMechanismName();

    /**
     * The SCRAM-SHA-256 Mechanism.
     *
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-256 SCRAM-SHA-256
     */
    public static final String SCRAM_SHA_256_MECHANISM = SCRAM_SHA_256.getMechanismName();

    /**
     * Mechanism property key for overriding the service name for GSSAPI authentication.
     *
     * @see #createGSSAPICredential(String)
     * @see #withMechanismProperty(String, Object)
     * @since 3.3
     */
    public static final String SERVICE_NAME_KEY = "SERVICE_NAME";

    /**
     * Mechanism property key for specifying whether to canonicalize the host name for GSSAPI authentication.
     *
     * @see #createGSSAPICredential(String)
     * @see #withMechanismProperty(String, Object)
     * @since 3.3
     */
    public static final String CANONICALIZE_HOST_NAME_KEY = "CANONICALIZE_HOST_NAME";

    /**
     * Mechanism property key for overriding the SaslClient properties for GSSAPI authentication.
     * <p>
     * The value of this property must be a {@code Map<String, Object>}.  In most cases there is no need to set this mechanism property.
     * But if an application does:
     * <ul>
     * <li>Generally it must set the {@link javax.security.sasl.Sasl#CREDENTIALS} property to an instance of
     * {@link org.ietf.jgss.GSSCredential}.</li>
     * <li>It's recommended that it set the {@link javax.security.sasl.Sasl#MAX_BUFFER} property to "0" to ensure compatibility with all
     * versions of MongoDB.</li>
     * </ul>
     *
     * @see #createGSSAPICredential(String)
     * @see #withMechanismProperty(String, Object)
     * @see javax.security.sasl.Sasl
     * @see javax.security.sasl.Sasl#CREDENTIALS
     * @see javax.security.sasl.Sasl#MAX_BUFFER
     * @since 3.3
     */
    public static final String JAVA_SASL_CLIENT_PROPERTIES_KEY = "JAVA_SASL_CLIENT_PROPERTIES";

    /**
     * Mechanism property key for controlling the {@link javax.security.auth.Subject} under which GSSAPI authentication executes.
     * <p>
     * See the {@link SubjectProvider} documentation for a description of how this mechanism property is used.
     * </p>
     * <p>
     * This property is ignored if the {@link #JAVA_SUBJECT_KEY} property is set.
     * </p>
     * @see SubjectProvider
     * @see #createGSSAPICredential(String)
     * @see #withMechanismProperty(String, Object)
     * @since 4.2
     */
    public static final String JAVA_SUBJECT_PROVIDER_KEY = "JAVA_SUBJECT_PROVIDER";

    /**
     * Mechanism property key for overriding the {@link javax.security.auth.Subject} under which GSSAPI authentication executes.
     *
     * @see #createGSSAPICredential(String)
     * @see #withMechanismProperty(String, Object)
     * @since 3.3
     */
    public static final String JAVA_SUBJECT_KEY = "JAVA_SUBJECT";

    /**
     * Mechanism property key for specifying the AWS session token.  The type of the value must be {@link String}.
     *
     * @see #createAwsCredential(String, char[])
     * @since 4.4
     */
    public static final String AWS_SESSION_TOKEN_KEY = "AWS_SESSION_TOKEN";

    /**
     * Mechanism property key for specifying a provider for an AWS credential, useful for refreshing a credential that could expire
     * during the lifetime of the {@code MongoClient} with which it is associated.  The type of the value must be a
     * {@code java.util.function.Supplier<com.mongodb.AwsCredential>}
     *
     * <p>
     * If this key is added to an AWS MongoCredential, the userName (i.e. accessKeyId), password (i.e. secretAccessKey), and
     * {@link MongoCredential#AWS_SESSION_TOKEN_KEY} value must all be null.
     * </p>
     *
     * @see #createAwsCredential(String, char[])
     * @see java.util.function.Supplier
     * @see AwsCredential
     * @since 4.4
     */
    @Beta(Reason.CLIENT)
    public static final String AWS_CREDENTIAL_PROVIDER_KEY = "AWS_CREDENTIAL_PROVIDER";

    /**
     * Mechanism property key for specifying the environment for OIDC, which is
     * the name of a built-in OIDC application environment integration to use
     * to obtain credentials. The value must be either "k8s", "gcp", or "azure".
     * This is an alternative to supplying a callback.
     * <p>
     * The "gcp" and "azure" environments require
     * {@link MongoCredential#TOKEN_RESOURCE_KEY} to be specified.
     * <p>
     * If this is provided,
     * {@link MongoCredential#OIDC_CALLBACK_KEY} and
     * {@link MongoCredential#OIDC_HUMAN_CALLBACK_KEY}
     * must not be provided.
     * <p>
     * The "k8s" environment will check the env vars
     * {@code AZURE_FEDERATED_TOKEN_FILE}, and then {@code AWS_WEB_IDENTITY_TOKEN_FILE},
     * for the token file path, and if neither is set will then use the path
     * {@code /var/run/secrets/kubernetes.io/serviceaccount/token}.
     *
     * @see #createOidcCredential(String)
     * @see MongoCredential#TOKEN_RESOURCE_KEY
     * @since 5.1
     */
    public static final String ENVIRONMENT_KEY = "ENVIRONMENT";

    /**
     * Mechanism property key for the OIDC callback.
     * This callback is invoked when the OIDC-based authenticator requests
     * a token. The type of the value must be {@link OidcCallback}.
     * {@link IdpInfo} will not be supplied to the callback,
     * and a {@linkplain com.mongodb.MongoCredential.OidcCallbackResult#getRefreshToken() refresh token}
     * must not be returned by the callback.
     * <p>
     * If this is provided, {@link MongoCredential#ENVIRONMENT_KEY}
     * and {@link MongoCredential#OIDC_HUMAN_CALLBACK_KEY}
     * must not be provided.
     *
     * @see #createOidcCredential(String)
     * @since 5.1
     */
    public static final String OIDC_CALLBACK_KEY = "OIDC_CALLBACK";

    /**
     * Mechanism property key for the OIDC human callback.
     * This callback is invoked when the OIDC-based authenticator requests
     * a token from the identity provider (IDP) using the IDP information
     * from the MongoDB server. The type of the value must be
     * {@link OidcCallback}.
     * <p>
     * If this is provided, {@link MongoCredential#ENVIRONMENT_KEY}
     * and {@link MongoCredential#OIDC_CALLBACK_KEY}
     * must not be provided.
     *
     * @see #createOidcCredential(String)
     * @since 5.1
     */
    public static final String OIDC_HUMAN_CALLBACK_KEY = "OIDC_HUMAN_CALLBACK";


    /**
     * Mechanism property key for a list of allowed hostnames or ip-addresses for MongoDB connections. Ports must be excluded.
     * The hostnames may include a leading "*." wildcard, which allows for matching (potentially nested) subdomains.
     * When MONGODB-OIDC authentication is attempted against a hostname that does not match any of list of allowed hosts
     * the driver will raise an error. The type of the value must be {@code List<String>}.
     *
     * @see MongoCredential#DEFAULT_ALLOWED_HOSTS
     * @see #createOidcCredential(String)
     * @since 5.1
     */
    public static final String ALLOWED_HOSTS_KEY = "ALLOWED_HOSTS";

    /**
     * The list of allowed hosts that will be used if no
     * {@link MongoCredential#ALLOWED_HOSTS_KEY} value is supplied.
     * The default allowed hosts are:
     * {@code "*.mongodb.net", "*.mongodb-qa.net", "*.mongodb-dev.net", "*.mongodbgov.net", "localhost", "127.0.0.1", "::1"}
     *
     * @see #createOidcCredential(String)
     * @since 5.1
     */
    public static final List<String> DEFAULT_ALLOWED_HOSTS = Collections.unmodifiableList(Arrays.asList(
            "*.mongodb.net", "*.mongodb-qa.net", "*.mongodb-dev.net", "*.mongodbgov.net", "localhost", "127.0.0.1", "::1"));

    /**
     * Mechanism property key for specifying the URI of the target resource (sometimes called the audience),
     * used in some OIDC environments.
     *
     * <p>A TOKEN_RESOURCE with a comma character must be given as a `MongoClient` configuration and not as
     * part of the connection string. The TOKEN_RESOURCE value can contain a colon character.
     *
     * @see MongoCredential#ENVIRONMENT_KEY
     * @see #createOidcCredential(String)
     * @since 5.1
     */
    public static final String TOKEN_RESOURCE_KEY = "TOKEN_RESOURCE";

    /**
     * Creates a MongoCredential instance with an unspecified mechanism.  The client will negotiate the best mechanism based on the
     * version of the server that the client is authenticating to.
     *
     * <p>If the server version is 4.0 or higher, the driver will negotiate with the server preferring the SCRAM-SHA-256 mechanism. 3.x
     * servers will authenticate using SCRAM-SHA-1, older servers will authenticate using the MONGODB_CR mechanism.</p>
     *
     * @param userName the user name
     * @param database the database where the user is defined
     * @param password the user's password
     * @return the credential
     *
     * @since 2.13
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-256 SCRAM-SHA-256
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1
     * @mongodb.driver.manual core/authentication/#mongodb-cr-authentication MONGODB-CR
     */
    public static MongoCredential createCredential(final String userName, final String database, final char[] password) {
        return new MongoCredential(null, userName, database, password);
    }

    /**
     * Creates a MongoCredential instance for the SCRAM-SHA-1 SASL mechanism. Use this method only if you want to ensure that
     * the driver uses the SCRAM-SHA-1 mechanism regardless of whether the server you are connecting to supports the
     * authentication mechanism.  Otherwise use the {@link #createCredential(String, String, char[])} method to allow the driver to
     * negotiate the best mechanism based on the server version.
     *
     * @param userName the non-null user name
     * @param source the source where the user is defined.
     * @param password the non-null user password
     * @return the credential
     * @see #createCredential(String, String, char[])
     *
     * @since 2.13
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1
     */
    public static MongoCredential createScramSha1Credential(final String userName, final String source, final char[] password) {
        return new MongoCredential(SCRAM_SHA_1, userName, source, password);
    }

    /**
     * Creates a MongoCredential instance for the SCRAM-SHA-256 SASL mechanism.
     *
     * @param userName the non-null user name
     * @param source the source where the user is defined.
     * @param password the non-null user password
     * @return the credential
     * @see #createCredential(String, String, char[])
     *
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-256 SCRAM-SHA-256
     */
    public static MongoCredential createScramSha256Credential(final String userName, final String source, final char[] password) {
        return new MongoCredential(SCRAM_SHA_256, userName, source, password);
    }

    /**
     * Creates a MongoCredential instance for the MongoDB X.509 protocol.
     *
     * @param userName the user name
     * @return the credential
     *
     * @since 2.12
     * @mongodb.driver.manual core/authentication/#x-509-certificate-authentication X-509
     */
    public static MongoCredential createMongoX509Credential(final String userName) {
        return new MongoCredential(MONGODB_X509, userName, "$external", null);
    }

    /**
     * Creates a MongoCredential instance for the MongoDB X.509 protocol where the distinguished subject name of the client certificate
     * acts as the userName.
     * <p>
     *     Available on MongoDB server versions &gt;= 3.4.
     * </p>
     * @return the credential
     *
     * @since 3.4
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual core/authentication/#x-509-certificate-authentication X-509
     */
    public static MongoCredential createMongoX509Credential() {
        return new MongoCredential(MONGODB_X509, null, "$external", null);
    }

    /**
     * Creates a MongoCredential instance for the PLAIN SASL mechanism.
     *
     * @param userName the non-null user name
     * @param source   the source where the user is defined.  This can be either {@code "$external"} or the name of a database.
     * @param password the non-null user password
     * @return the credential
     *
     * @since 2.12
     * @mongodb.driver.manual core/authentication/#ldap-proxy-authority-authentication PLAIN
     */
    public static MongoCredential createPlainCredential(final String userName, final String source, final char[] password) {
        return new MongoCredential(PLAIN, userName, source, password);
    }

    /**
     * Creates a MongoCredential instance for the GSSAPI SASL mechanism.
     * <p>
     * To override the default service name of {@code "mongodb"}, add a mechanism property with the name {@code "SERVICE_NAME"}.
     * <p>
     * To force canonicalization of the host name prior to authentication, add a mechanism property with the name
     * {@code "CANONICALIZE_HOST_NAME"} with the value{@code true}.
     * <p>
     * To override the {@link javax.security.auth.Subject} with which the authentication executes, add a mechanism property with the name
     * {@code "JAVA_SUBJECT"} with the value of a {@code Subject} instance.
     * <p>
     * To override the properties of the {@link javax.security.sasl.SaslClient} with which the authentication executes, add a mechanism
     * property with the name {@code "JAVA_SASL_CLIENT_PROPERTIES"} with the value of a {@code Map<String, Object>} instance containing the
     * necessary properties.  This can be useful if the application is customizing the default
     * {@link javax.security.sasl.SaslClientFactory}.
     *
     * @param userName the non-null user name
     * @return the credential
     * @mongodb.server.release 2.4
     * @mongodb.driver.manual core/authentication/#kerberos-authentication GSSAPI
     * @see #withMechanismProperty(String, Object)
     * @see #SERVICE_NAME_KEY
     * @see #CANONICALIZE_HOST_NAME_KEY
     * @see #JAVA_SUBJECT_KEY
     * @see #JAVA_SASL_CLIENT_PROPERTIES_KEY
     */
    public static MongoCredential createGSSAPICredential(final String userName) {
        return new MongoCredential(GSSAPI, userName, "$external", null);
    }

    /**
     * Creates a MongoCredential instance for the MONGODB-AWS mechanism.
     *
     * @param userName the user name, which may be null.  This maps to the AWS accessKeyId
     * @param password the user password, which may be null if the userName is also null.  This maps to the AWS secretAccessKey.
     * @return the credential
     * @since 4.1
     * @see #withMechanismProperty(String, Object)
     * @see #AWS_SESSION_TOKEN_KEY
     * @see #AWS_CREDENTIAL_PROVIDER_KEY
     * @mongodb.server.release 4.4
     */
    public static MongoCredential createAwsCredential(@Nullable final String userName, @Nullable final char[] password) {
        return new MongoCredential(MONGODB_AWS, userName, "$external", password);
    }

    /**
     * Creates a MongoCredential instance for the MONGODB-OIDC mechanism.
     *
     * @param userName the user name, which may be null. This is the OIDC principal name.
     * @return the credential
     * @since 5.1
     * @see #withMechanismProperty(String, Object)
     * @see #ENVIRONMENT_KEY
     * @see #TOKEN_RESOURCE_KEY
     * @see #OIDC_CALLBACK_KEY
     * @see #OIDC_HUMAN_CALLBACK_KEY
     * @see #ALLOWED_HOSTS_KEY
     * @mongodb.server.release 7.0
     */
    public static MongoCredential createOidcCredential(@Nullable final String userName) {
        return new MongoCredential(MONGODB_OIDC, userName, "$external", null);
    }

    /**
     * Creates a new MongoCredential as a copy of this instance, with the specified mechanism property added.
     *
     * @param key   the key to the property, which is treated as case-insensitive
     * @param value the value of the property
     * @param <T>   the property type
     * @return the credential
     * @since 2.12
     */
    public <T> MongoCredential withMechanismProperty(final String key, final T value) {
        return new MongoCredential(this, key, value);
    }

    /**
     * Creates a new MongoCredential with the set mechanism. The existing mechanism must be null.
     *
     * @param mechanism the mechanism to set
     * @return the credential
     * @since 3.8
     */
    public MongoCredential withMechanism(final AuthenticationMechanism mechanism) {
        if (this.mechanism != null) {
            throw new IllegalArgumentException("Mechanism already set");
        }
        return new MongoCredential(mechanism, userName, source, password, mechanismProperties);
    }

    /**
     * Constructs a new instance using the given mechanism, userName, source, and password
     *
     * @param mechanism the authentication mechanism
     * @param userName  the user name
     * @param source    the source of the user name, typically a database name
     * @param password  the password
     */
    MongoCredential(@Nullable final AuthenticationMechanism mechanism, @Nullable final String userName, final String source,
                    @Nullable final char[] password) {
        this(mechanism, userName, source, password, Collections.emptyMap());
    }

    MongoCredential(@Nullable final AuthenticationMechanism mechanism, @Nullable final String userName, final String source,
                    @Nullable final char[] password, final Map<String, Object> mechanismProperties) {

        if (mechanism == MONGODB_OIDC) {
            validateOidcCredentialConstruction(source, mechanismProperties);
            validateCreateOidcCredential(password);
        }

        if (userName == null && !Arrays.asList(MONGODB_X509, MONGODB_AWS, MONGODB_OIDC).contains(mechanism)) {
            throw new IllegalArgumentException("username can not be null");
        }

        if (mechanism == null && password == null) {
            throw new IllegalArgumentException("Password can not be null when the authentication mechanism is unspecified");
        }

        if (mechanismRequiresPassword(mechanism) && password == null) {
            throw new IllegalArgumentException("Password can not be null for " + mechanism + " mechanism");
        }

        if ((mechanism == GSSAPI || mechanism == MONGODB_X509) && password != null) {
            throw new IllegalArgumentException("Password must be null for the " + mechanism + " mechanism");
        }

        if (mechanism == MONGODB_AWS && userName != null && password == null) {
            throw new IllegalArgumentException("Password can not be null when username is provided for " + mechanism + " mechanism");
        }

        this.mechanism = mechanism;
        this.userName = userName;
        this.source = notNull("source", source);

        this.password = password != null ? password.clone() : null;
        this.mechanismProperties = new HashMap<>(mechanismProperties);
    }

    private boolean mechanismRequiresPassword(@Nullable final AuthenticationMechanism mechanism) {
        return mechanism == PLAIN || mechanism == SCRAM_SHA_1 || mechanism == SCRAM_SHA_256;
    }

    /**
     * Constructs a new instance using the given credential plus an additional mechanism property.
     *
     * @param from                   the credential to copy from
     * @param mechanismPropertyKey   the new mechanism property key
     * @param mechanismPropertyValue the new mechanism property value
     * @param <T>                    the mechanism property type
     */
    <T> MongoCredential(final MongoCredential from, final String mechanismPropertyKey, final T mechanismPropertyValue) {
        this(from.mechanism, from.userName, from.source, from.password, mapWith(from.mechanismProperties, notNull(
                "mechanismPropertyKey", mechanismPropertyKey).toLowerCase(), mechanismPropertyValue));
    }

    private static <T> Map<String, Object> mapWith(final Map<String, Object> map, final String key, final T value) {
        HashMap<String, Object> result = new HashMap<>(map);
        result.put(key, value);
        return result;
    }

    /**
     * Gets the mechanism
     *
     * @return the mechanism.
     */
    @Nullable
    public String getMechanism() {
        return mechanism == null ? null : mechanism.getMechanismName();
    }

    /**
     * Gets the mechanism
     *
     * @return the mechanism.
     * @since 3.0
     */
    @Nullable
    public AuthenticationMechanism getAuthenticationMechanism() {
        return mechanism;
    }

    /**
     * Gets the user name
     *
     * @return the user name.
     */
    @Nullable
    public String getUserName() {
        return userName;
    }

    /**
     * Gets the source of the user name, typically the name of the database where the user is defined.
     *
     * @return the source of the user name.  Can never be null.
     */
    public String getSource() {
        return source;
    }

    /**
     * Gets the password.
     *
     * @return the password.  Can be null for some mechanisms.
     */
    @Nullable
    public char[] getPassword() {
        if (password == null) {
            return null;
        }
        return password.clone();
    }

    /**
     * Get the value of the given key to a mechanism property, or defaultValue if there is no mapping.
     *
     * @param key          the mechanism property key, which is treated as case-insensitive
     * @param defaultValue the default value, if no mapping exists
     * @param <T>          the value type
     * @return the mechanism property value
     * @since 2.12
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T getMechanismProperty(final String key, @Nullable final T defaultValue) {
        notNull("key", key);

        T value = (T) mechanismProperties.get(key.toLowerCase());
        return (value == null) ? defaultValue : value;

    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoCredential that = (MongoCredential) o;

        if (mechanism != that.mechanism) {
            return false;
        }
        if (!Arrays.equals(password, that.password)) {
            return false;
        }
        if (!source.equals(that.source)) {
            return false;
        }
        if (!Objects.equals(userName, that.userName)) {
            return false;
        }
        if (!mechanismProperties.equals(that.mechanismProperties)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = mechanism != null ? mechanism.hashCode() : 0;
        result = 31 * result + (userName != null ? userName.hashCode() : 0);
        result = 31 * result + source.hashCode();
        result = 31 * result + (password != null ? Arrays.hashCode(password) : 0);
        result = 31 * result + mechanismProperties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MongoCredential{"
                + "mechanism=" + mechanism
                + ", userName='" + userName + '\''
                + ", source='" + source + '\''
                + ", password=<hidden>"
                + ", mechanismProperties=<hidden>"
                + '}';
    }

    /**
     * The context for the {@link OidcCallback#onRequest(OidcCallbackContext) OIDC request callback}.
     *
     * @since 5.1
     */
    @Evolving
    public interface OidcCallbackContext {
        /**
         * @return Convenience method to obtain the {@linkplain MongoCredential#getUserName() username}.
         */
        @Nullable
        String getUserName();

        /**
         * @return The timeout that this callback must complete within.
         */
        Duration getTimeout();

        /**
         * @return The OIDC callback API version. Currently, version 1.
         */
        int getVersion();

        /**
         * @return The OIDC Identity Provider's configuration that can be used
         * to acquire an Access Token, or null if not using a
         * {@linkplain MongoCredential#OIDC_HUMAN_CALLBACK_KEY human callback.}
         */
        @Nullable
        IdpInfo getIdpInfo();

        /**
         * @return The OIDC Refresh token supplied by a prior callback invocation,
         * or null if no token was supplied, or if not using a
         * {@linkplain MongoCredential#OIDC_HUMAN_CALLBACK_KEY human callback.}
         */
        @Nullable
        String getRefreshToken();
    }

    /**
     * This callback is invoked when the OIDC-based authenticator requests
     * tokens from the identity provider.
     * <p>
     * It does not have to be thread-safe, unless it is provided to multiple
     * MongoClients.
     *
     * @since 5.1
     */
    public interface OidcCallback {
        /**
         * @param context The context.
         * @return The response produced by an OIDC Identity Provider
         */
        OidcCallbackResult onRequest(OidcCallbackContext context);
    }

    /**
     * The OIDC Identity Provider's configuration that can be used to acquire an Access Token.
     *
     * @since 5.1
     */
    @Evolving
    public interface IdpInfo {
        /**
         * @return URL which describes the Authorization Server. This identifier is the
         * iss of provided access tokens, and is viable for RFC8414 metadata
         * discovery and RFC9207 identification.
         */
        String getIssuer();

        /**
         * @return Unique client ID for this OIDC client.
         */
        @Nullable
        String getClientId();

        /**
         * @return Additional scopes to request from Identity Provider. Immutable.
         */
        List<String> getRequestScopes();
    }

    /**
     * The OIDC credential information.
     *
     * @since 5.1
     */
    public static final class OidcCallbackResult {

        private final String accessToken;

        private final Duration expiresIn;

        @Nullable
        private final String refreshToken;


        /**
         * An access token that does not expire.
         * @param accessToken The OIDC access token.
         */
        public OidcCallbackResult(final String accessToken) {
            this(accessToken, Duration.ZERO, null);
        }

        /**
         * @param accessToken The OIDC access token.
         * @param expiresIn Time until the access token expires.
         *                  A {@linkplain Duration#isZero() zero-length} duration
         *                  means that the access token does not expire.
         */
        public OidcCallbackResult(final String accessToken, final Duration expiresIn) {
            this(accessToken, expiresIn, null);
        }

        /**
         * @param accessToken The OIDC access token.
         * @param expiresIn Time until the access token expires.
         *                  A {@linkplain Duration#isZero() zero-length} duration
         *                  means that the access token does not expire.
         * @param refreshToken The refresh token. If null, refresh will not be attempted.
         */
        public OidcCallbackResult(final String accessToken, final Duration expiresIn,
                @Nullable final String refreshToken) {
            notNull("accessToken", accessToken);
            notNull("expiresIn", expiresIn);
            if (expiresIn.isNegative()) {
                throw new IllegalArgumentException("expiresIn must not be a negative value");
            }
            this.accessToken = accessToken;
            this.expiresIn = expiresIn;
            this.refreshToken = refreshToken;
        }

        /**
         * @return The OIDC access token.
         */
        public String getAccessToken() {
            return accessToken;
        }

        /**
         * @return The OIDC refresh token. If null, refresh will not be attempted.
         */
        @Nullable
        public String getRefreshToken() {
            return refreshToken;
        }
    }
}

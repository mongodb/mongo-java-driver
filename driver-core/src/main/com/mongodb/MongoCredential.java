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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.AuthenticationMechanism.MONGODB_CR;
import static com.mongodb.AuthenticationMechanism.MONGODB_X509;
import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.assertions.Assertions.notNull;

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
     * The MongoDB Challenge Response mechanism.
     * @mongodb.driver.manual core/authentication/#mongodb-cr-authentication MONGODB-CR
     * @deprecated This mechanism was replaced by {@link #SCRAM_SHA_1_MECHANISM} in MongoDB 3.0, and is now deprecated
     */
    @Deprecated
    public static final String MONGODB_CR_MECHANISM = AuthenticationMechanism.MONGODB_CR.getMechanismName();

    /**
     * The GSSAPI mechanism.  See the <a href="http://tools.ietf.org/html/rfc4752">RFC</a>.
     *
     * @mongodb.server.release 2.4
     * @mongodb.driver.manual core/authentication/#kerberos-authentication GSSAPI
     */
    public static final String GSSAPI_MECHANISM = AuthenticationMechanism.GSSAPI.getMechanismName();

    /**
     * The PLAIN mechanism.  See the <a href="http://www.ietf.org/rfc/rfc4616.txt">RFC</a>.
     *
     * @since 2.12
     * @mongodb.server.release 2.6
     * @mongodb.driver.manual core/authentication/#ldap-proxy-authority-authentication PLAIN
     */
    public static final String PLAIN_MECHANISM = AuthenticationMechanism.PLAIN.getMechanismName();

    /**
     * The MongoDB X.509
     *
     * @since 2.12
     * @mongodb.server.release 2.6
     * @mongodb.driver.manual core/authentication/#x-509-certificate-authentication X-509
     */
    public static final String MONGODB_X509_MECHANISM = AuthenticationMechanism.MONGODB_X509.getMechanismName();

    /**
     * The SCRAM-SHA-1 Mechanism.
     *
     * @since 2.13
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1
     */
    public static final String SCRAM_SHA_1_MECHANISM = AuthenticationMechanism.SCRAM_SHA_1.getMechanismName();

    /**
     * The SCRAM-SHA-256 Mechanism.
     *
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-256 SCRAM-SHA-256
     */
    public static final String SCRAM_SHA_256_MECHANISM = AuthenticationMechanism.SCRAM_SHA_256.getMechanismName();

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
     *
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
     * Mechanism property key for overriding the {@link javax.security.auth.Subject} under which GSSAPI authentication executes.
     *
     * @see #createGSSAPICredential(String)
     * @see #withMechanismProperty(String, Object)
     * @since 3.3
     */
    public static final String JAVA_SUBJECT_KEY = "JAVA_SUBJECT";

    /**
     * Creates a MongoCredential instance with an unspecified mechanism.  The client will negotiate the best mechanism based on the
     * version of the server that the client is authenticating to.  If the server version is 3.0 or higher,
     * the driver will authenticate using the SCRAM-SHA-1 mechanism.  Otherwise, the driver will authenticate using the MONGODB_CR
     * mechanism.
     *
     *
     * @param userName the user name
     * @param database the database where the user is defined
     * @param password the user's password
     * @return the credential
     *
     * @since 2.13
     * @mongodb.driver.manual core/authentication/#mongodb-cr-authentication MONGODB-CR
     * @mongodb.driver.manual core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1
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
     * Creates a MongoCredential instance for the MongoDB Challenge Response protocol. Use this method only if you want to ensure that
     * the driver uses the MONGODB_CR mechanism regardless of whether the server you are connecting to supports a more secure
     * authentication mechanism.  Otherwise use the {@link #createCredential(String, String, char[])} method to allow the driver to
     * negotiate the best mechanism based on the server version.
     *
     * @param userName the user name
     * @param database the database where the user is defined
     * @param password the user's password
     * @return the credential
     * @see #createCredential(String, String, char[])
     * @mongodb.driver.manual core/authentication/#mongodb-cr-authentication MONGODB-CR
     * @deprecated MONGODB-CR was replaced by SCRAM-SHA-1 in MongoDB 3.0, and is now deprecated. Use
     * the {@link #createCredential(String, String, char[])} factory method instead.
     */
    @Deprecated
    public static MongoCredential createMongoCRCredential(final String userName, final String database, final char[] password) {
        return new MongoCredential(MONGODB_CR, userName, database, password);
    }

    /**
     * Creates a MongoCredential instance for the MongoDB X.509 protocol.
     *
     * @param userName the user name
     * @return the credential
     *
     * @since 2.12
     * @mongodb.server.release 2.6
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
     * @mongodb.server.release 2.6
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
     * property with the name {@code "JAVA_SASL_CLIENT_PROPERTIES"} with the value of a {@code Map<String, Object} instance containing the
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
     * Constructs a new instance using the given mechanism, userName, source, and password
     *
     * @param mechanism the authentication mechanism
     * @param userName  the user name
     * @param source    the source of the user name, typically a database name
     * @param password  the password
     */
    MongoCredential(@Nullable final AuthenticationMechanism mechanism, @Nullable final String userName, final String source,
                    @Nullable final char[] password) {
        if (mechanism != MONGODB_X509 && userName == null) {
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

        this.mechanism = mechanism;
        this.userName = userName;
        this.source = notNull("source", source);

        this.password = password != null ? password.clone() : null;
        this.mechanismProperties = Collections.emptyMap();
    }

    @SuppressWarnings("deprecation")
    private boolean mechanismRequiresPassword(@Nullable final AuthenticationMechanism mechanism) {
        return mechanism == PLAIN || mechanism == MONGODB_CR || mechanism == SCRAM_SHA_1;
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
        notNull("mechanismPropertyKey", mechanismPropertyKey);

        this.mechanism = from.mechanism;
        this.userName = from.userName;
        this.source = from.source;
        this.password = from.password;
        this.mechanismProperties = new HashMap<String, Object>(from.mechanismProperties);
        this.mechanismProperties.put(mechanismPropertyKey.toLowerCase(), mechanismPropertyValue);
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
     * @return the user name.  Can never be null.
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
        if (userName != null ? !userName.equals(that.userName) : that.userName != null) {
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
               + ", mechanismProperties=" + mechanismProperties
               + '}';
    }
}


/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

/**
 * Represents credentials to authenticate to a mongo server,as well as the source of the credentials and the authentication mechanism to
 * use.
 *
 * @since 2.11.0
 */
@Immutable
public final class MongoCredential {

    /**
     * The MongoDB Challenge Response mechanism.
     */
    public static final String MONGODB_CR_MECHANISM = "MONGODB-CR";

    /**
     * The GSSAPI mechanism.  See the <a href="http://tools.ietf.org/html/rfc4752">RFC</a>.
     *
     * @mongodb.server.release 2.4
     */
    public static final String GSSAPI_MECHANISM = "GSSAPI";

    /**
     * The PLAIN mechanism.  See the <a href="http://www.ietf.org/rfc/rfc4616.txt">RFC</a>.
     *
     * @mongodb.server.release 2.6
     */
    public static final String PLAIN_MECHANISM = "PLAIN";

    /**
     * The MongoDB X.509
     *
     * @mongodb.server.release 2.6
     */
    public static final String MONGODB_X509_MECHANISM = "MONGODB-X509";

    private final org.mongodb.MongoCredential proxied;

    /**
     * Creates a MongoCredential instance for the MongoDB Challenge Response protocol.
     *
     * @param userName the user name
     * @param database the database where the user is defined
     * @param password the user's password
     * @return the credential
     */
    public static MongoCredential createMongoCRCredential(final String userName, final String database, final char[] password) {
        return new MongoCredential(org.mongodb.MongoCredential.createMongoCRCredential(userName, database, password));
    }

    /**
     * Creates a MongoCredential instance for the MongoDB X.509 protocol.
     *
     * @param userName the user name
     * @return the credential
     *
     * @mongodb.server.release 2.6
     */
    public static MongoCredential createMongoX509Credential(final String userName) {
        return new MongoCredential(org.mongodb.MongoCredential.createMongoX509Credential(userName));
    }

    /**
     * Creates a MongoCredential instance for the PLAIN SASL mechanism.
     *
     * @param userName the non-null user name
     * @param source   the source where the user is defined.  This can be either {@code "$external"} or the name of a database.
     * @param password the non-null user password
     * @return the credential
     *
     * @mongodb.server.release 2.6
     */
    public static MongoCredential createPlainCredential(final String userName, final String source, final char[] password) {
        return new MongoCredential(org.mongodb.MongoCredential.createPlainCredential(userName, source, password));
    }


    /**
     * Creates a MongoCredential instance for the GSSAPI SASL mechanism.  To override the default service name of {@code "mongodb"}, add a
     * mechanism property with the name {@code "SERVICE_NAME"}. To force canonicalization of the host name prior to authentication, add a
     * mechanism property with the name {@code "CANONICALIZE_HOST_NAME"} with the value{@code true}.
     *
     * @param userName the non-null user name
     * @return the credential
     * @see #withMechanismProperty(String, Object)
     *
     * @mongodb.server.release 2.4
     */
    public static MongoCredential createGSSAPICredential(final String userName) {
        return new MongoCredential(org.mongodb.MongoCredential.createGSSAPICredential(userName));
    }

    /**
     * Creates a new MongoCredential as a copy of this instance, with the specified mechanism property added.
     *
     * @param key   the key to the property
     * @param value the value of the property
     * @param <T>   the property type
     * @return the credential
     */
    public <T> MongoCredential withMechanismProperty(final String key, final T value) {
        return new MongoCredential(proxied.withMechanismProperty(key, value));
    }

    /**
     * Constructs a new instance using the given mechanism, userName, source, and password
     *
     * @param proxied the proxied credential
     */
    MongoCredential(final org.mongodb.MongoCredential proxied) {
        this.proxied = proxied;
    }

    /**
     * Gets the mechanism
     *
     * @return the mechanism.
     */
    public String getMechanism() {
        return proxied.getMechanism().getMechanismName();
    }

    /**
     * Gets the user name
     *
     * @return the user name.  Can never be null.
     */
    public String getUserName() {
        return proxied.getUserName();
    }

    /**
     * Gets the source of the user name, typically the name of the database where the user is defined.
     *
     * @return the user name.  Can never be null.
     */
    public String getSource() {
        return proxied.getSource();
    }

    /**
     * Gets the password.
     *
     * @return the password.  Can be null for some mechanisms.
     */
    public char[] getPassword() {
        return proxied.getPassword();
    }

    /**
     * Get the value of the given key to a mechanism property, or defaultValue if there is no mapping.
     *
     * @param key          the mechanism property key
     * @param defaultValue the default value, if no mapping exists
     * @param <T>          the value type
     * @return the mechanism property value
     */
    public <T> T getMechanismProperty(final String key, final T defaultValue) {
        return proxied.getMechanismProperty(key, defaultValue);
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

        if (!proxied.equals(that.proxied)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return proxied.hashCode();
    }

    @Override
    public String toString() {
        return proxied.toString();
    }

    public org.mongodb.MongoCredential toNew() {
        return proxied;
    }
}


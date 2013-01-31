/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import org.bson.util.annotations.Immutable;

import java.util.Arrays;

/**
 * Represents credentials to authenticate to a mongo server, as well as the source of the credentials and
 * the authentication mechanism to use.
 *
 * @since 2.11.0
 */
@Immutable
public class MongoCredential {

    private final MongoAuthenticationMechanism mechanism;
    private final String userName;
    private final char[] password;
    private final String source;

    /**
     * Constructs a new instance using the given user name and password and the default mechanism and source.
     *
     * @param userName the user name
     * @param password the password
     */
    public MongoCredential(final String userName, final char[] password) {
        this(userName, password, MongoAuthenticationMechanism.MONGO_CR);
    }

    /**
     * Constructs a new instance using the given user name, password and source and the default mechanism.
     *
     * @param userName the user name
     * @param password the password
     * @param source   the source of the credentials
     */
    public MongoCredential(final String userName, final char[] password, String source) {
        this(userName, password, MongoAuthenticationMechanism.MONGO_CR, source);
    }

    /**
     * Constructs a new instance using the given user name, password and mechanism and the default source for that mechanism.
     *
     * @param userName the user name
     * @param password the password
     * @param mechanism the mechanism to use for authentication
     */
    public MongoCredential(final String userName, final char[] password, MongoAuthenticationMechanism mechanism) {
        this(userName, password, mechanism, null);
    }

    /**
     * Constructs a new instance using the given user name and mechanism.  This really only applies to the GSSAPI
     * mechanism, since it's the only one that doesn't require a password.
     *
     * @param userName the user name
     * @param mechanism the mechanism to use for authentication
     */
    public MongoCredential(final String userName, final MongoAuthenticationMechanism mechanism) {
        this(userName, null, mechanism);
    }

    /**
     /**
     * Constructs a new instance using the given user name, password, mechanism, and source.
     *
     * @param userName the user name
     * @param password the password
     * @param mechanism the mechanism
     * @param source   the source of the credentials
     */
    public MongoCredential(final String userName, final char[] password, MongoAuthenticationMechanism mechanism, String source) {
        if (userName == null) {
            throw new IllegalArgumentException();
        }
        if (mechanism == null) {
            throw new IllegalArgumentException();
        }

        if (mechanism == MongoAuthenticationMechanism.MONGO_CR && password == null) {
            throw new IllegalArgumentException("password can not be null for " + MongoAuthenticationMechanism.MONGO_CR);
        }

        if (mechanism == MongoAuthenticationMechanism.GSSAPI && password != null) {
            throw new IllegalArgumentException("password must be null for " + MongoAuthenticationMechanism.GSSAPI);
        }

        this.userName = userName;
        this.password = password;
        this.source = source != null ? source : getDefaultDatabase(mechanism);
        this.mechanism = mechanism;
    }

    /**
     * Gets the mechanism
     *
     * @return the mechanism.
     */
    public MongoAuthenticationMechanism getMechanism() {
        return mechanism;
    }

    /**
     * Gets the user name
     *
     * @return the user name.  Can never be null.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Gets the password.
     *
     * @return the password.  Can be null for some mechanisms.
     */
    public char[] getPassword() {
        if (password == null) {
            return null;
        }
        return password.clone();
    }

    /**
     * Gets the source, which is usually the name of the database that the credentials are stored in.
     *
     * @return the source.
     */
    public String getSource() {
        return source;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongoCredential that = (MongoCredential) o;

        if (!Arrays.equals(password, that.password)) return false;
        if (mechanism != that.mechanism) return false;
        if (!source.equals(that.source)) return false;
        if (!userName.equals(that.userName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = mechanism.hashCode();
        result = 31 * result + userName.hashCode();
        result = 31 * result + (password != null ? Arrays.hashCode(password) : 0);
        result = 31 * result + source.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MongoCredentials{" +
                "mechanism=" + mechanism +
                ", userName='" + userName +
                ", password=" + "<hidden>" +
                ", source='" + source +
                '}';
    }

    private String getDefaultDatabase(final MongoAuthenticationMechanism mechanism) {
        if (mechanism == null) {
            return "admin";
        } else {
            return mechanism.getDefaultSource();
        }
    }
}

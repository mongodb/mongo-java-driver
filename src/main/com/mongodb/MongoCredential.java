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
public final class MongoCredential {

    /**
     * The GSSAPI mechanism.  See the <a href="http://tools.ietf.org/html/rfc4752">RFC</a>.
     */
    public static final String GSSAPI_MECHANISM = "GSSAPI";

    /**
     * The MongoDB Challenge Response mechanism.
     */
    public static final String MONGODB_CR_MECHANISM = "MONGODB-CR";


    private final String mechanism;
    private final String userName;
    private final String source;
    private final char[] password;

    /**
     * Creates a MongoCredential instance for the MongoDB Challenge Response protocol.
     *
     * @param userName the user name
     * @param database the database where the user is defined
     * @param password the user's password
     * @return the credential
     */
    public static MongoCredential createMongoCRCredential(String userName, String database, char[] password) {
        return new MongoCredential(MONGODB_CR_MECHANISM, userName, database, password);
    }

    /**
     * Creates a MongoCredential instance for the GSSAPI SASL mechanism.
     *
     * @param userName the user name
     * @return the credential
     */
    public static MongoCredential createGSSAPICredential(String userName) {
        return new MongoCredential(GSSAPI_MECHANISM, userName, "$external", null);
    }

    /**
     *
     * Constructs a new instance using the given mechanism, userName, source, and password
     *
     * @param mechanism the authentication mechanism
     * @param userName the user name
     * @param source the source of the user name, typically a database name
     * @param password the password
     */
    MongoCredential(final String mechanism, final String userName, final String source, final char[] password) {
        if (mechanism == null) {
            throw new IllegalArgumentException("mechanism can not be null");
        }

        if (userName == null) {
            throw new IllegalArgumentException("username can not be null");
        }

        if (mechanism.equals(MONGODB_CR_MECHANISM) && password == null) {
            throw new IllegalArgumentException("Password can not be null for " + MONGODB_CR_MECHANISM + " mechanism");
        }

        if (mechanism.equals(GSSAPI_MECHANISM) && password != null) {
            throw new IllegalArgumentException("Password must be null for the " + GSSAPI_MECHANISM + " mechanism");
        }

        this.mechanism = mechanism;
        this.userName = userName;
        this.source = source;
        this.password = password != null ? password.clone() : null;
    }

    /**
     * Gets the mechanism
     *
     * @return the mechanism.
     */
    public String getMechanism() {
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
    public char[] getPassword() {
        if (password == null) {
            return null;
        }
        return password.clone();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongoCredential that = (MongoCredential) o;

        if (!mechanism.equals(that.mechanism)) return false;
        if (!Arrays.equals(password, that.password)) return false;
        if (!source.equals(that.source)) return false;
        if (!userName.equals(that.userName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = mechanism.hashCode();
        result = 31 * result + userName.hashCode();
        result = 31 * result + source.hashCode();
        result = 31 * result + (password != null ? Arrays.hashCode(password) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MongoCredential{" +
                "mechanism='" + mechanism + '\'' +
                ", userName='" + userName + '\'' +
                ", source='" + source + '\'' +
                ", password=<hidden>"  +
                '}';
    }
}

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
 * the authentication protocol to use.
 *
 * @since 2.11.0
 */
@Immutable
public class MongoCredentials {

    private final Protocol protocol;
    private final String userName;
    private final char[] password;
    private final String source;

    /**
     * An enumeration of the supported authentication protocols.
     */
    public enum Protocol {
        /**
         * The GSSAPI protocol, to support Kerberos v5 via a SASL-based authentication protocol
         */
        GSSAPI {
            /**
             * The default source for GSSAPI is a reserved name that doesn't correspond to any database.
             * @return the default source.
             */
            @Override
            public String getDefaultSource() {
                return "$external";
            }
        },
        /**
         * Negotiate the strongest available protocol available.  This is the default protocol.
         */
        NEGOTIATE {
            /**
             * The default source is the "admin" database.
             * @return
             */
            @Override
            public String getDefaultSource() {
                return "admin";
            }
        };

        /**
         * Gets the default source for this mechanism, usually a database name.
         *
         * @return the default database for this protocol
         */
        public abstract String getDefaultSource();
    }

    /**
     * Constructs a new instance using the given user name and password and the default protocol and source.
     *
     * @param userName the user name
     * @param password the password
     */
    public MongoCredentials(final String userName, final char[] password) {
        this(userName, password, Protocol.NEGOTIATE);
    }

    /**
     * Constructs a new instance using the given user name, password and source and the default protocol.
     *
     * @param userName the user name
     * @param password the password
     * @param source   the source of the credentials
     */
    public MongoCredentials(final String userName, final char[] password, String source) {
        this(userName, password, Protocol.NEGOTIATE, source);
    }

    /**
     * Constructs a new instance using the given user name, password and protocol and the default source for that protocol.
     *
     * @param userName the user name
     * @param password the password
     * @param protocol the protocol to use for authentication
     */
    public MongoCredentials(final String userName, final char[] password, Protocol protocol) {
        this(userName, password, protocol, null);
    }

    /**
     * Constructs a new instance using the given user name and protocol.  This really only applies to the GSSAPI
     * protocol, since it's the only one that doesn't require a password.
     *
     * @param userName the user name
     * @param protocol the protocol to use for authentication
     */
    public MongoCredentials(final String userName, final Protocol protocol) {
        this(userName, null, protocol);
    }

    /**
     /**
     * Constructs a new instance using the given user name, password, protocol, and source.
     *
     * @param userName the user name
     * @param password the password
     * @param protocol the protocol
     * @param source   the source of the credentials
     */
    public MongoCredentials(final String userName, final char[] password, Protocol protocol, String source) {
        if (userName == null) {
            throw new IllegalArgumentException();
        }
        if (protocol == null) {
            throw new IllegalArgumentException();
        }

        if (protocol == Protocol.NEGOTIATE && password == null) {
            throw new IllegalArgumentException("password can not be null for " + Protocol.NEGOTIATE);
        }

        if (protocol == Protocol.GSSAPI && password != null) {
            throw new IllegalArgumentException("password must be null for " + Protocol.GSSAPI);
        }

        this.userName = userName;
        this.password = password;
        this.source = source != null ? source : getDefaultDatabase(protocol);
        this.protocol = protocol;
    }

    /**
     * Gets the mechanism
     *
     * @return the mechanism.  Can be null if the mechanism should be negotiated.
     */
    public Protocol getProtocol() {
        return protocol;
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
     * Gets the source, which is usually the name of the database that the credentials are stored in..
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

        final MongoCredentials that = (MongoCredentials) o;

        if (!Arrays.equals(password, that.password)) return false;
        if (protocol != that.protocol) return false;
        if (!source.equals(that.source)) return false;
        if (!userName.equals(that.userName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = protocol.hashCode();
        result = 31 * result + userName.hashCode();
        result = 31 * result + (password != null ? Arrays.hashCode(password) : 0);
        result = 31 * result + source.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MongoCredentials{" +
                "protocol=" + protocol +
                ", userName='" + userName +
                ", password=" + "<hidden>" +
                ", source='" + source +
                '}';
    }

    private String getDefaultDatabase(final Protocol protocol) {
        if (protocol == null) {
            return "admin";
        } else {
            return protocol.getDefaultSource();
        }
    }
}

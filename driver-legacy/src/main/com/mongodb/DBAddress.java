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

import java.net.InetAddress;

/**
 * Represents a database address, which includes the properties of ServerAddress (host and port) and adds a database name.
 *
 * @mongodb.driver.manual reference/default-mongodb-port/ MongoDB Ports
 * @mongodb.driver.manual reference/connection-string/ MongoDB Connection String
 * @deprecated This class is no longer needed, as the driver does not rely on it for anything anymore.  Use {@link ServerAddress} instead.
 */
@Deprecated
public class DBAddress extends ServerAddress {
    private static final long serialVersionUID = -813211264765778133L;
    private final String _db;

    /**
     * Creates a new address. Accepts as the parameter format:
     *
     * <ul>
     *     <li><i>name</i> "mydb"</li>
     *     <li><i>&lt;host&gt;/name</i> "127.0.0.1/mydb"</li>
     *     <li><i>&lt;host&gt;:&lt;port&gt;/name</i> "127.0.0.1:8080/mydb"</li>
     * </ul>
     *
     * @param urlFormat the URL-formatted host and port
     * @mongodb.driver.manual reference/connection-string/ MongoDB Connection String
     * @see MongoClientURI
     */
    public DBAddress(final String urlFormat) {
        super(_getHostSection(urlFormat));

        _check(urlFormat, "urlFormat");
        _db = _fixName(_getDBSection(urlFormat));

        _check(getHost(), "host");
        _check(_db, "db");
    }

    static String _getHostSection(final String urlFormat) {
        if (urlFormat == null) {
            throw new NullPointerException("urlFormat can't be null");
        }

        int idx = urlFormat.indexOf("/");
        if (idx >= 0) {
            return urlFormat.substring(0, idx);
        }
        return null;
    }

    static String _getDBSection(final String urlFormat) {
        if (urlFormat == null) {
            throw new NullPointerException("urlFormat can't be null");
        }

        int idx = urlFormat.indexOf("/");
        if (idx >= 0) {
            return urlFormat.substring(idx + 1);
        }
        return urlFormat;
    }

    static String _fixName(final String name) {
        return name.replace('.', '-');
    }

    /**
     * Create a DBAddress using the host and port from an existing DBAddress, and connected to a given database.
     *
     * @param other        an existing {@code DBAddress} that gives the host and port
     * @param databaseName the database to which to connect
     */
    public DBAddress(final DBAddress other, final String databaseName) {
        this(other.getHost(), other.getPort(), databaseName);
    }

    /**
     * Creates a DBAddress for the given database on the given host.
     *
     * @param host         host name
     * @param databaseName database name
     */
    public DBAddress(final String host, final String databaseName) {
        this(host, defaultPort(), databaseName);
    }

    /**
     * Creates a DBAddress for the given database on the given host at the given port.
     *
     * @param host         host name
     * @param port         database port
     * @param databaseName database name
     */
    public DBAddress(final String host, final int port, final String databaseName) {
        super(host, port);
        _db = databaseName.trim();
    }

    /**
     * @param inetAddress  host address
     * @param port         database port
     * @param databaseName database name
     */
    public DBAddress(final InetAddress inetAddress, final int port, final String databaseName) {
        super(inetAddress, port);
        _check(databaseName, "name");
        _db = databaseName.trim();
    }

    static void _check(final String thing, final String name) {
        if (thing == null) {
            throw new NullPointerException(name + " can't be null ");
        }

        String trimmedThing = thing.trim();
        if (trimmedThing.length() == 0) {
            throw new IllegalArgumentException(name + " can't be empty");
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() + _db.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof DBAddress) {
            DBAddress a = (DBAddress) other;
            return a.getPort() == getPort()
                   && a._db.equals(_db)
                   && a.getHost().equals(getHost());
        } else if (other instanceof ServerAddress) {
            return other.equals(this);
        }
        return false;
    }

    /**
     * Creates a DBAddress pointing to a different database on the same server.
     *
     * @param name database name
     * @return the DBAddress for the given name with the same host and port as this
     * @throws MongoException if failed
     */
    public DBAddress getSister(final String name) {
        return new DBAddress(getHost(), getPort(), name);
    }

    /**
     * Gets the database name
     *
     * @return the database name
     */
    public String getDBName() {
        return _db;
    }

    /**
     * Gets a String representation of address as host:port/databaseName.
     *
     * @return this address
     */
    @Override
    public String toString() {
        return super.toString() + "/" + _db;
    }

}

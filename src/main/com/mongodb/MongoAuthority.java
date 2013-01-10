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

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents the authority to which this client is connecting.  It includes
 * both the server address(es) and optional authentication credentials.  The class name is informed by the
 * <a href="http://tools.ietf.org/html/rfc3986#section-3.2">URI RFC</a>, which refers to the username/host/port
 * part of the URI as the "authority".
 *
 * @since 2.11.0
 */
@Immutable
public class MongoAuthority {
    private final ServerAddress serverAddress;
    private final MongoCredentialsStore credentialsStore;
    private final List<ServerAddress> serverAddresses;

    /**
     * Constructs an instance with a single server address.  This will be a direct connection, even
     * if it's part of a replica set.
     *
     * @param serverAddress the server address of a mongo server
     */
    public MongoAuthority(final ServerAddress serverAddress) {
        this(serverAddress, (MongoCredentials) null);
    }

    /**
     * Constructs an instance with a list of server addresses.  This may either be a list of mongos servers
     * or a list of members of a replica set
     *
     * @param serverAddresses the server addresses
     */
    public MongoAuthority(final List<ServerAddress> serverAddresses) {
        this(serverAddresses, (MongoCredentials) null);
    }

    /**
     * Constructs an instance with a single server address and authentication credentials.  This will be a direct connection,
     * even if it's part of a replica set.
     *
     * @param serverAddress the server address of a mongo server
     */
    public MongoAuthority(final ServerAddress serverAddress, MongoCredentials credentials) {
        this(serverAddress, new MongoCredentialsStore(credentials));
    }

    /**
     * Constructs an instance with a list of server addresses, which may either be a list of mongos servers
     * or a list of members of a replica set, and authentication credentials.
     *
     * @param serverAddresses the server addresses
     */
    public MongoAuthority(final List<ServerAddress> serverAddresses, MongoCredentials credentials) {
        this(serverAddresses, new MongoCredentialsStore(credentials));
    }

    /**
     * Constructs an instance with a single server address and a store of authentication credentials.
     * This will be a direct connection, even if it's part of a replica set.
     *
     * @param serverAddress the server address of a mongo server
     */
    public MongoAuthority(final ServerAddress serverAddress, MongoCredentialsStore credentialsStore) {
        this.serverAddress = serverAddress;
        this.credentialsStore = credentialsStore;
        this.serverAddresses = null;
    }

    /**
     * Constructs an instance with a list of server addresses, which may either be a list of mongos servers
     * or a list of members of a replica set, and a store of authentication credentials.
     *
     * @param serverAddresses the server addresses
     * @param credentialsStore the credentials store
     */
    public MongoAuthority(final List<ServerAddress> serverAddresses, MongoCredentialsStore credentialsStore) {
        this.serverAddresses = new ArrayList<ServerAddress>(serverAddresses);
        this.credentialsStore = credentialsStore;
        this.serverAddress = null;
    }

    /**
     * Returns whether this is a direct connection to a single server.
     *
     * @return true if a direct connection
     */
    public boolean isDirect() {
        return serverAddress != null;
    }

    /**
     * Returns the single server address of a direction connection, or null if it's not a direction connection.
     * @return the server address
     * @see com.mongodb.MongoAuthority#isDirect()
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Returns the list of server addresses if this is not a direction connection.  or null if it is.
     * @return the server address list
     * @see com.mongodb.MongoAuthority#isDirect()
     */
    public List<ServerAddress> getServerAddresses() {
        return new ArrayList<ServerAddress>(serverAddresses);
    }

    /**
     * Gets the credentials store.  If this instance was constructed with a single credential, this store will
     * contain it.
     * @return the credentials store
     */
    public MongoCredentialsStore getCredentialsStore() {
        return credentialsStore;
    }
}

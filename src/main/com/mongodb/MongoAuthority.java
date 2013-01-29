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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class represents the authority to which this client is connecting.  It includes
 * both the server address(es) and optional authentication credentials.  The class name is informed by the
 * <a href="http://tools.ietf.org/html/rfc3986#section-3.2">URI RFC</a>, which refers to the username/host/port
 * part of a URI as the "authority".
 *
 * @since 2.11.0
 */
@Immutable
class MongoAuthority {
    private final Type type;
    private final List<ServerAddress> serverAddresses;
    private final MongoCredentialsStore credentialsStore;

    /**
     * Enumeration of the connection types.
     */
    enum Type {
        Direct,
        Set
    }

    /**
     *
     * @param serverAddress
     * @return
     */
    public static MongoAuthority direct(ServerAddress serverAddress) {
        return direct(serverAddress, (MongoCredential) null);
    }

    /**
     *
     * @param serverAddress
     * @param credentials
     * @return
     */
    public static MongoAuthority direct(ServerAddress serverAddress, MongoCredential credentials) {
        return direct(serverAddress, new MongoCredentialsStore(credentials));
    }

    /**
     *
     * @param serverAddress
     * @param credentialsStore
     * @return
     */
    public static MongoAuthority direct(ServerAddress serverAddress, MongoCredentialsStore credentialsStore) {
        return new MongoAuthority(serverAddress, credentialsStore);
    }

    /**
     *
     * @param serverAddresses
     * @return
     */
    public static MongoAuthority dynamicSet(List<ServerAddress> serverAddresses) {
        return dynamicSet(serverAddresses, (MongoCredential) null);
    }

    /**
     *
     * @param serverAddresses
     * @param credentials
     * @return
     */
    public static MongoAuthority dynamicSet(List<ServerAddress> serverAddresses, MongoCredential credentials) {
        return dynamicSet(serverAddresses, new MongoCredentialsStore(credentials));
    }

    /**
     *
     * @param serverAddresses
     * @param credentialsStore
     * @return
     */
    public static MongoAuthority dynamicSet(List<ServerAddress> serverAddresses, MongoCredentialsStore credentialsStore) {
        return new MongoAuthority(serverAddresses, Type.Set, credentialsStore);
    }

    /**
     * Constructs an instance with a single server address and a store of authentication credentials.
     * This will be a direct connection, even if it's part of a replica set.
     *
     * @param serverAddress the server address of a mongo server
     */
    private MongoAuthority(final ServerAddress serverAddress, MongoCredentialsStore credentialsStore) {
        if (serverAddress == null) {
            throw new IllegalArgumentException("serverAddress can not be null");
        }

        if (credentialsStore == null) {
            throw new IllegalArgumentException("credentialsStore can not be null");
        }

        this.serverAddresses = Arrays.asList(serverAddress);
        this.credentialsStore = credentialsStore;
        this.type = Type.Direct;
    }

    /**
     * Constructs an instance with a list of server addresses, which may either be a list of mongos servers
     * or a list of members of a replica set, and a store of authentication credentials.
     *
     * @param serverAddresses  the server addresses
     * @param credentialsStore the credentials store
     */
    private MongoAuthority(final List<ServerAddress> serverAddresses, Type type, MongoCredentialsStore credentialsStore) {
        if (serverAddresses == null) {
            throw new IllegalArgumentException("serverAddresses can not be null");
        }

        if (credentialsStore == null) {
            throw new IllegalArgumentException("credentialsStore can not be null");
        }

        if (type == null) {
            throw new IllegalArgumentException("type can not be null");
        }

        if (type == Type.Direct) {
            throw new IllegalArgumentException("type can not be Direct with a list of server addresses");
        }

        this.type = type;
        this.serverAddresses = new ArrayList<ServerAddress>(serverAddresses);
        this.credentialsStore = credentialsStore;
    }

    /**
     * Returns the list of server addresses.
     *
     * @return the server address list
     */
    public List<ServerAddress> getServerAddresses() {
        return serverAddresses == null ? null : Collections.unmodifiableList(serverAddresses);
    }

    /**
     * Gets the credentials store.  If this instance was constructed with a single credential, this store will
     * contain it.
     *
     * @return the credentials store
     */
    public MongoCredentialsStore getCredentialsStore() {
        return credentialsStore;
    }

    /**
     * Gets the authority type
     *
     * @return the authority type
     */
    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongoAuthority that = (MongoAuthority) o;

        if (!credentialsStore.equals(that.credentialsStore)) return false;
        if (!serverAddresses.equals(that.serverAddresses)) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = credentialsStore.hashCode();
        result = 31 * result + serverAddresses.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MongoAuthority{" +
                "type=" + type +
                ", serverAddresses=" + serverAddresses +
                ", credentials=" + credentialsStore +
                '}';
    }
}

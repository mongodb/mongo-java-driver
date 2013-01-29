/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.bson.util.annotations.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An effectively immutable store of credentials to mongo servers.  It enforces the invariant that there can be at most
 * one credentials for each database.  It allows at most one credentials with a null database.
 *
 * There is still a package-protected method to add a new credentials to the store, but it's only there
 * to support DB.authenticate, which allows you to add new credentials at any point during the life of a MongoClient.
 *
 * @since 2.11.0
 */
@ThreadSafe
class MongoCredentialsStore {
    private final Map<String, MongoCredential> credentialsMap = new HashMap<String, MongoCredential>();
    private volatile Set<String> allDatabasesWithCredentials = new HashSet<String>();

    /**
     * Creates an empty store
     */
    public MongoCredentialsStore() {
    }

    /**
     * Creates a store with a single credentials.
     *
     * @param credentials A single credentials, which may be null.
     */
    public MongoCredentialsStore(MongoCredential credentials) {
        if (credentials == null) {
            return;
        }
        add(credentials);
    }

    /**
     * Creates a store with the list of credentials.
     *
     * @param credentialsList The list of credentials
     */
    public MongoCredentialsStore(Iterable<MongoCredential> credentialsList) {
        if (credentialsList == null) {
            return;
        }
        for (MongoCredential cur : credentialsList) {
           add(cur);
        }
    }

    /**
     * Adds a new credentials.
     *
     * @param credentials the new credentials
     * @throws IllegalArgumentException if there already exist different credentials for the same database
     */
    synchronized void add(MongoCredential credentials) {
        MongoCredential existingCredentials = credentialsMap.get(credentials.getSource());

        if (existingCredentials != null) {
            if (existingCredentials.equals(credentials)) {
                return;
            }
            throw new IllegalArgumentException("Can't add more than one credentials for the same database");
        }

        credentialsMap.put(credentials.getSource(), credentials);
        allDatabasesWithCredentials = new HashSet<String>(allDatabasesWithCredentials);
        allDatabasesWithCredentials.add(credentials.getSource());
    }

    /**
     * Gets the set of databases for which there are credentials stored.
     *
     * @return an unmodifiable set of database names.  Can contain the null string.
     */
    public Set<String> getDatabases() {
        return Collections.unmodifiableSet(allDatabasesWithCredentials);
    }

    /**
     * Gets the stored credentials for the given database.
     *
     * @param database the database.  This can be null, to get the credentials with the null database.
     * @return the credentials for the given database.  Can be null if not are stored.
     */
    public synchronized MongoCredential get(String database) {
        return credentialsMap.get(database);
    }

    /**
     * Gets the MongoCredentials in this map as a List
     * @return the list of credentials
     */
    public synchronized List<MongoCredential> asList() {
       return new ArrayList<MongoCredential>(credentialsMap.values());
    }

    @Override
    public synchronized boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongoCredentialsStore that = (MongoCredentialsStore) o;

        if (!credentialsMap.equals(that.credentialsMap)) return false;

        return true;
    }

    @Override
    public synchronized int hashCode() {
        return credentialsMap.hashCode();
    }

    @Override
    public String toString() {
        return "{" +
                "credentials=" + credentialsMap +
                '}';
    }
}

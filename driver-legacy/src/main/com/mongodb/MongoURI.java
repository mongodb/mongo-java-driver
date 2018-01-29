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

import java.util.List;


/**
 * <p>Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a> which can be used to create a Mongo instance. The URI
 * describes the hosts to be used and options.</p>
 *
 * <p>This class has been superseded by {@code MongoClientURI}, and may be removed in a future release.</p>
 *
 * @see MongoClientURI
 * @deprecated Replaced by {@link MongoClientURI}
 */
@Deprecated
public class MongoURI {

    /**
     * The prefix for mongodb URIs.
     */
    public static final String MONGODB_PREFIX = "mongodb://";
    private final MongoClientURI proxied;

    private final MongoOptions options;

    /**
     * Creates a MongoURI from a string.
     *
     * @param uri the URI
     * @mongodb.driver.manual reference/connection-string Connection String URI Format
     * @deprecated Replaced by {@link MongoClientURI#MongoClientURI(String)}
     */
    @Deprecated
    public MongoURI(final String uri) {
        this.proxied = new MongoClientURI(uri, MongoClientOptions.builder()
                                                                 .connectionsPerHost(10)
                                                                 .writeConcern(WriteConcern.UNACKNOWLEDGED)
        );
        options = new MongoOptions(proxied.getOptions());
    }

    /**
     * Create a new MongoURI from a MongoClientURI.  This class is deprecated, use {@link com.mongodb.MongoClientURI}.
     *
     * @param proxied the MongoClientURI to wrap with this deprecated class. * @deprecated Replaced by {@link MongoClientURI})
     */
    @Deprecated
    public MongoURI(final MongoClientURI proxied) {
        this.proxied = proxied;
        options = new MongoOptions(proxied.getOptions());
    }

    // ---------------------------------

    /**
     * Gets the username.
     *
     * @return the username
     */
    public String getUsername() {
        return proxied.getUsername();
    }

    /**
     * Gets the password.
     *
     * @return the password
     */
    public char[] getPassword() {
        return proxied.getPassword();
    }

    /**
     * Gets the list of hosts.
     *
     * @return the list of hosts
     */
    public List<String> getHosts() {
        return proxied.getHosts();
    }

    /**
     * Gets the database name.
     *
     * @return the database name
     */
    public String getDatabase() {
        return proxied.getDatabase();
    }

    /**
     * Gets the collection name.
     *
     * @return the collection name
     */
    public String getCollection() {
        return proxied.getCollection();
    }

    /**
     * Gets the credentials.
     *
     * @since 2.11.0
     * @return the MongoCredential for conneting to MongoDB servers.
     */
    public MongoCredential getCredentials() {
        return proxied.getCredentials();
    }

    /**
     * Gets the options. This method will return the same instance of {@code MongoOptions} for every call, so it's possible to mutate the
     * returned instance to change the defaults.
     *
     * @return the mongo options
     */
    @SuppressWarnings("deprecation")
    public MongoOptions getOptions() {
        return options;
    }

    /**
     * Creates a Mongo instance based on the URI.
     *
     * @return a new Mongo instance.  There is no caching, so each call will create a new instance, each of which must be closed manually.
     * @throws MongoException if there's a failure
     */
    @SuppressWarnings("deprecation")
    public Mongo connect() {
        // Note: we can't change this to new MongoClient(this) as that would silently change the default write concern.
        return new Mongo(this);
    }

    /**
     * Returns the DB object from a newly created Mongo instance based on this URI.
     *
     * @return the database specified in the URI.  This will implicitly create a new Mongo instance, which must be closed manually.
     * @throws MongoException if there's a failure
     */
    public DB connectDB() {
        return connect().getDB(getDatabase());
    }

    /**
     * Returns the URI's DB object from a given Mongo instance.
     *
     * @param mongo the Mongo instance to get the database from.
     * @return the database specified in this URI
     */
    public DB connectDB(final Mongo mongo) {
        return mongo.getDB(getDatabase());
    }

    /**
     * Returns the URI's Collection from a given DB object.
     *
     * @param db the database to get the collection from
     * @return the collection specified in this URI
     */
    public DBCollection connectCollection(final DB db) {
        return db.getCollection(getCollection());
    }

    /**
     * Returns the URI's Collection from a given Mongo instance
     *
     * @param mongo the mongo instance to get the collection from
     * @return the collection specified in this URI
     */
    public DBCollection connectCollection(final Mongo mongo) {
        return connectDB(mongo).getCollection(getCollection());
    }

    @Override
    public String toString() {
        return proxied.toString();
    }

    MongoClientURI toClientURI() {
        return proxied;
    }
}

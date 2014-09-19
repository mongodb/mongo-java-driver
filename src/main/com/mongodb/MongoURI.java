/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import java.net.UnknownHostException;
import java.util.List;


/**
 * This class has been replaced by {@link com.mongodb.MongoClientURI}, and the documentation has been moved to that class.
 *
 * @see MongoClientURI
 * @see MongoOptions for the default values of all options
 */
public class MongoURI {

    /**
     * The prefix for mongodb URIs.
     */
    public static final String MONGODB_PREFIX = "mongodb://";

    private final MongoClientURI mongoClientURI;
    private final MongoOptions mongoOptions;

    /**
     * Creates a MongoURI from a string.
     * @param uri the URI
     * @dochub connections
     *
     * @deprecated Replaced by {@link MongoClientURI#MongoClientURI(String)}
     *
     */
    @Deprecated
    public MongoURI( String uri ) {
        this.mongoClientURI = new MongoClientURI(uri, new MongoClientOptions.Builder().legacyDefaults());
        mongoOptions = new MongoOptions(mongoClientURI.getOptions());
    }

    @Deprecated
    public MongoURI(final MongoClientURI mongoClientURI) {
        this.mongoClientURI = mongoClientURI;
        mongoOptions = new MongoOptions(mongoClientURI.getOptions());
    }

    // ---------------------------------

    /**
     * Gets the username
     * @return
     */
    public String getUsername(){
        return mongoClientURI.getUsername();
    }

    /**
     * Gets the password
     * @return
     */
    public char[] getPassword(){
        return mongoClientURI.getPassword();
    }

    /**
     * Gets the list of hosts
     * @return
     */
    public List<String> getHosts(){
        return mongoClientURI.getHosts();
    }

    /**
     * Gets the database name
     * @return
     */
    public String getDatabase(){
        return mongoClientURI.getDatabase();
    }

    /**
     * Gets the collection name
     * @return
     */
    public String getCollection(){
        return mongoClientURI.getCollection();
    }

    /**
     * Gets the credentials
     *
     * @since 2.11.0
     */
    public MongoCredential getCredentials() {
        return mongoClientURI.getCredentials();
    }

    /**
     * Gets the options.  This method will return the same instance of {@code MongoOptions} for every call, so it's
     * possible to mutate the returned instance to change the defaults.
     * @return the mongo options
     */
    public MongoOptions getOptions(){
        return mongoOptions;
    }

    /**
     * creates a Mongo instance based on the URI
     * @return a new Mongo instance.  There is no caching, so each call will create a new instance, each of which
     * must be closed manually.
     * @throws MongoException
     * @throws UnknownHostException
     */
    @SuppressWarnings("deprecation")
    public Mongo connect()
            throws UnknownHostException {
        // TODO caching?
        // Note: we can't change this to new MongoClient(this) as that would silently change the default write concern.
        return new Mongo(this);
    }

    /**
     * returns the DB object from a newly created Mongo instance based on this URI
     * @return the database specified in the URI.  This will implicitly create a new Mongo instance,
     * which must be closed manually.
     * @throws MongoException
     * @throws UnknownHostException
     */
    public DB connectDB() throws UnknownHostException {
        return connect().getDB(getDatabase());
    }

    /**
     * returns the URI's DB object from a given Mongo instance
     * @param mongo the Mongo instance to get the database from.
     * @return the database specified in this URI
     */
    public DB connectDB( Mongo mongo ){
        return mongo.getDB( getDatabase() );
    }

    /**
     * returns the URI's Collection from a given DB object
     * @param db the database to get the collection from
     * @return
     */
    public DBCollection connectCollection( DB db ){
        return db.getCollection( getCollection() );
    }

    /**
     * returns the URI's Collection from a given Mongo instance
     * @param mongo the mongo instance to get the collection from
     * @return the collection specified in this URI
     */
    public DBCollection connectCollection( Mongo mongo ){
        return connectDB( mongo ).getCollection( getCollection() );
    }

    // ---------------------------------

    @Override
    public String toString() {
        return mongoClientURI.toString();
    }

    MongoClientURI toClientURI() {
        return mongoClientURI;
    }
}

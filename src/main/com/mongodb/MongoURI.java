/**
 * Copyright (C) 2008 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
 * Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a>
 * which can be used to create a Mongo instance. The URI describes the hosts to
 * be used and options.
 * <p>
 * This class has been superseded by <{@code MongoClientURI}, and may be deprecated in a future release.
 * <p>The format of the URI is:
 * <pre>
 *   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
 * </pre>
 * <ul>
 *   <li>{@code mongodb://} is a required prefix to identify that this is a string in the standard connection format.</li>
 *   <li>{@code username:password@} are optional.  If given, the driver will attempt to login to a database after
 *       connecting to a database server.</li>
 *   <li>{@code host1} is the only required part of the URI.  It identifies a server address to connect to.</li>
 *   <li>{@code :portX} is optional and defaults to :27017 if not provided.</li>
 *   <li>{@code /database} is the name of the database to login to and thus is only relevant if the
 *       {@code username:password@} syntax is used. If not specified the "admin" database will be used by default.</li>
 *   <li>{@code ?options} are connection options. Note that if {@code database} is absent there is still a {@code /}
 *       required between the last host and the {@code ?} introducing the options. Options are name=value pairs and the pairs
 *       are separated by "&amp;". For backwards compatibility, ";" is accepted as a separator in addition to "&amp;",
 *       but should be considered as deprecated.</li>
 * </ul>
 * <p>
 *     The Java driver supports the following options (case insensitive):
 * <p>
 *     Replica set configuration:
 * </p>
 * <ul>
 *   <li>{@code replicaSet=name}: Implies that the hosts given are a seed list, and the driver will attempt to find
 *        all members of the set.</li>
 * </ul>
 * <p>Connection Configuration:</p>
 * <ul>
 *   <li>{@code connectTimeoutMS=ms}: How long a connection can take to be opened before timing out.</li>
 *   <li>{@code socketTimeoutMS=ms}: How long a send or receive on a socket can take before timing out.</li>
 * </ul>
 * <p>Connection pool configuration:</p>
 * <ul>
 *   <li>{@code maxPoolSize=n}: The maximum number of connections in the connection pool.</li>
 *   <li>{@code waitQueueMultiple=n} : this multiplier, multiplied with the maxPoolSize setting, gives the maximum number of
 *       threads that may be waiting for a connection to become available from the pool.  All further threads will get an
 *       exception right away.</li>
 *   <li>{@code waitQueueTimeoutMS=ms}: The maximum wait time in milliseconds that a thread may wait for a connection to
 *       become available.</li>
 * </ul>
 * <p>Write concern configuration:</p>
 * <ul>
 *   <li>{@code safe=true|false}
 *     <ul>
 *       <li>{@code true}: the driver sends a getLastError command after every update to ensure that the update succeeded
 *           (see also {@code w} and {@code wtimeoutMS}).</li>
 *       <li>{@code false}: the driver does not send a getLastError command after every update.</li>
 *     </ul>
 *   </li>
 *   <li>{@code w=wValue}
 *     <ul>
 *       <li>The driver adds { w : wValue } to the getLastError command. Implies {@code safe=true}.</li>
 *       <li>wValue is typically a number, but can be any string in order to allow for specifications like
 *           {@code "majority"}</li>
 *     </ul>
 *   </li>
 *   <li>{@code wtimeoutMS=ms}
 *     <ul>
 *       <li>The driver adds { wtimeout : ms } to the getlasterror command. Implies {@code safe=true}.</li>
 *       <li>Used in combination with {@code w}</li>
 *     </ul>
 *   </li>
 * </ul>
 * <p>Read preference configuration:</p>
 * <ul>
 *   <li>{@code slaveOk=true|false}: Whether a driver connected to a replica set will send reads to slaves/secondaries.</li>
 *   <li>{@code readPreference=enum}: The read preference for this connection.  If set, it overrides any slaveOk value.
 *     <ul>
 *       <li>Enumerated values:
 *         <ul>
 *           <li>{@code primary}</li>
 *           <li>{@code primaryPreferred}</li>
 *           <li>{@code secondary}</li>
 *           <li>{@code secondaryPreferred}</li>
 *           <li>{@code nearest}</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 *   <li>{@code readPreferenceTags=string}.  A representation of a tag set as a comma-separated list of colon-separated
 *       key-value pairs, e.g. {@code "dc:ny,rack:1}".  Spaces are stripped from beginning and end of all keys and values.
 *       To specify a list of tag sets, using multiple readPreferenceTags,
 *       e.g. {@code readPreferenceTags=dc:ny,rack:1;readPreferenceTags=dc:ny;readPreferenceTags=}
 *     <ul>
 *        <li>Note the empty value for the last one, which means match any secondary as a last resort.</li>
 *        <li>Order matters when using multiple readPreferenceTags.</li>
 *     </ul>
 *   </li>
 * </ul>
 * @see MongoClientURI
 * @see MongoOptions for the default values for all options
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

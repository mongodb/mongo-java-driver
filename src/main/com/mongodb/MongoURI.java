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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;


/**
 * Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a>
 * which can be used to create a Mongo instance. The URI describes the hosts to
 * be used and options.
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
 * @see MongoOptions for the default values for all options
 */
public class MongoURI {

    public static final String MONGODB_PREFIX = "mongodb://";

    /**
     * Creates a MongoURI described by a String.
     * examples
     *   mongodb://127.0.0.1
     *   mongodb://fred:foobar@127.0.0.1/
     * @param uri the URI
     * @dochub connections
     */
    public MongoURI( String uri ){
        _uri = uri;
        if ( ! uri.startsWith( MONGODB_PREFIX ) )
            throw new IllegalArgumentException( "uri needs to start with " + MONGODB_PREFIX );

        uri = uri.substring(MONGODB_PREFIX.length());

        String serverPart;
        String nsPart;
        String optionsPart;

        {
            int idx = uri.lastIndexOf( "/" );
            if ( idx < 0 ){
                serverPart = uri;
                nsPart = null;
                optionsPart = null;
            }
            else {
                serverPart = uri.substring( 0 , idx );
                nsPart = uri.substring( idx + 1 );

                idx = nsPart.indexOf( "?" );
                if ( idx >= 0 ){
                    optionsPart = nsPart.substring( idx + 1 );
                    nsPart = nsPart.substring( 0 , idx );
                }
                else {
                    optionsPart = null;
                }

            }
        }

        { // _username,_password,_hosts
            List<String> all = new LinkedList<String>();


            int idx = serverPart.indexOf( "@" );

            if ( idx > 0 ){
                String authPart = serverPart.substring( 0 , idx );
                serverPart = serverPart.substring( idx + 1 );

                idx = authPart.indexOf( ":" );
                _username = authPart.substring( 0, idx );
                _password = authPart.substring( idx + 1 ).toCharArray();
            }
            else {
                _username = null;
                _password = null;
            }

            for ( String s : serverPart.split( "," ) )
                all.add( s );

            _hosts = Collections.unmodifiableList( all );
        }

        if ( nsPart != null ){ // _database,_collection
            int idx = nsPart.indexOf( "." );
            if ( idx < 0 ){
                _database = nsPart;
                _collection = null;
            }
            else {
                _database = nsPart.substring( 0 , idx );
                _collection = nsPart.substring( idx + 1 );
            }
        }
        else {
            _database = null;
            _collection = null;
        }

        if ( optionsPart != null && optionsPart.length() > 0 ) parseOptions( optionsPart );
    }

    @SuppressWarnings("deprecation")
    private void parseOptions( String optionsPart ){
        String readPreferenceType = null;
        DBObject firstTagSet = null;
        List<DBObject> remainingTagSets = new ArrayList<DBObject>();

        for ( String _part : optionsPart.split( "&|;" ) ){
            int idx = _part.indexOf( "=" );
            if ( idx >= 0 ){
                String key = _part.substring( 0, idx ).toLowerCase();
                String value = _part.substring( idx + 1 );
                if ( key.equals( "maxpoolsize" ) ) _options.connectionsPerHost = Integer.parseInt( value );
                else if ( key.equals( "minpoolsize" ) )
                    LOGGER.warning( "Currently No support in Java driver for Min Pool Size." );
                else if ( key.equals( "waitqueuemultiple" ) )
                    _options.threadsAllowedToBlockForConnectionMultiplier = Integer.parseInt( value );
                else if ( key.equals( "waitqueuetimeoutms" ) ) _options.maxWaitTime = Integer.parseInt( value );
                else if ( key.equals( "connecttimeoutms" ) ) _options.connectTimeout = Integer.parseInt( value );
                else if ( key.equals( "sockettimeoutms" ) ) _options.socketTimeout = Integer.parseInt( value );
                else if ( key.equals( "autoconnectretry" ) ) _options.autoConnectRetry = _parseBoolean( value );
                else if ( key.equals( "slaveok" ) ) _options.slaveOk = _parseBoolean( value );
                else if ( key.equals( "safe" ) ) _options.safe = _parseBoolean( value );
                else if ( key.equals( "w" ) ) _options.w = Integer.parseInt( value );
                else if ( key.equals( "wtimeout" ) ) _options.wtimeout = Integer.parseInt( value );
                else if ( key.equals( "fsync" ) ) _options.fsync = _parseBoolean( value );
                else if ( key.equals( "readpreference")) readPreferenceType = value;
                else if ( key.equals( "readpreferencetags")) {
                    DBObject tagSet = getTagSet(value.trim());
                    if (firstTagSet == null) {
                        firstTagSet = tagSet;
                    } else {
                        remainingTagSets.add(tagSet);
                    }
                }
                else LOGGER.warning("Unknown or Unsupported Option '" + key + "'");
            }
        }

        if (readPreferenceType != null) {
            if (firstTagSet == null) {
               _options.readPreference = ReadPreference.valueOf(readPreferenceType);
            }
            else {
               _options.readPreference = ReadPreference.valueOf(readPreferenceType, firstTagSet,
                       remainingTagSets.toArray(new DBObject[remainingTagSets.size()]));
            }
        }
    }

    private DBObject getTagSet(String tagSetString) {
        DBObject tagSet = new BasicDBObject();
        if (tagSetString.length() > 0) {
            for (String tag : tagSetString.split(",")) {
                String[] tagKeyValuePair = tag.split(":");
                if (tagKeyValuePair.length != 2) {
                    throw new IllegalArgumentException("Bad read preference tags: " + tagSetString);
                }
                tagSet.put(tagKeyValuePair[0].trim(), tagKeyValuePair[1].trim());
            }
        }
        return tagSet;
     }

    boolean _parseBoolean( String _in ){
        String in = _in.trim();
        if ( in != null && in.length() > 0 && ( in.equals( "1" ) || in.toLowerCase().equals( "true" ) || in.toLowerCase()
                                                                                                         .equals( "yes" ) ) )
            return true;
        else return false;
    }

    // ---------------------------------

    /**
     * Gets the username
     * @return
     */
    public String getUsername(){
        return _username;
    }

    /**
     * Gets the password
     * @return
     */
    public char[] getPassword(){
        return _password;
    }

    /**
     * Gets the list of hosts
     * @return
     */
    public List<String> getHosts(){
        return _hosts;
    }

    /**
     * Gets the database name
     * @return
     */
    public String getDatabase(){
        return _database;
    }

    /**
     * Gets the collection name
     * @return
     */
    public String getCollection(){
        return _collection;
    }

    /**
     * Gets the options
     * @return
     */
    public MongoOptions getOptions(){
        return _options;
    }

    /**
     * creates a Mongo instance based on the URI
     * @return
     * @throws MongoException
     * @throws UnknownHostException
     */
    public Mongo connect()
        throws UnknownHostException {
        // TODO caching?
        return new Mongo( this );
    }

    /**
     * returns the DB object from a newly created Mongo instance based on this URI
     * @return
     * @throws MongoException
     * @throws UnknownHostException
     */
    public DB connectDB()
        throws UnknownHostException {
        // TODO auth
        return connect().getDB( _database );
    }

    /**
     * returns the URI's DB object from a given Mongo instance
     * @param m
     * @return
     */
    public DB connectDB( Mongo m ){
        // TODO auth
        return m.getDB( _database );
    }

    /**
     * returns the URI's Collection from a given DB object
     * @param db
     * @return
     */
    public DBCollection connectCollection( DB db ){
        return db.getCollection( _collection );
    }

    /**
     * returns the URI's Collection from a given Mongo instance
     * @param m
     * @return
     */
    public DBCollection connectCollection( Mongo m ){
        return connectDB( m ).getCollection( _collection );
    }

    // ---------------------------------

    final String _username;
    final char[] _password;
    final List<String> _hosts;
    final String _database;
    final String _collection;

    final MongoOptions _options = new MongoOptions();

    final String _uri;

    static final Logger LOGGER = Logger.getLogger( "com.mongodb.MongoURI" );

    @Override
    public String toString() {
        return _uri;
    }
}

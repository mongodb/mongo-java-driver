// MongoURI.java

package com.mongodb;

import java.net.*;
import java.util.*;
import java.util.logging.*;

/**
 * Represents a URI which can be used to create a Mongo instance.
 * The URI describes the hosts to be used and options.
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

    private void parseOptions( String optionsPart ){
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
                else LOGGER.warning( "Unknown or Unsupported Option '" + value + "'" );
            }
        }
    }

    boolean _parseBoolean( String _in ){
        String in = _in.trim();
        if ( in != null && !in.isEmpty() && ( in.equals( "1" ) || in.toLowerCase().equals( "true" ) || in.toLowerCase()
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
        throws MongoException , UnknownHostException {
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
        throws MongoException , UnknownHostException {
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

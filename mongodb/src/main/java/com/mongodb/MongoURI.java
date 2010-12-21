// MongoURI.java

package com.mongodb;

import java.net.*;
import java.util.*;

public class MongoURI {
    
    /**
     * examples
     *   mongodb://localhost
     *   mongodb://fred:foobar@localhost/
     *   @dochub connections
     */
    public MongoURI( String uri ){
        _uri = uri;
        if ( ! uri.startsWith( "mongodb://" ) )
            throw new IllegalArgumentException( "uri needs to start with mongodb://" );
        
        uri = uri.substring( 10 );
        
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
            
            
            if ( serverPart.indexOf( "@" ) > 0 ){
                int idx = serverPart.indexOf( "@" );
                _username = serverPart.substring( 0 , idx );
                serverPart = serverPart.substring( idx + 1 );

                idx = serverPart.indexOf( ":" );
                _password = serverPart.substring( 0 , idx ).toCharArray();
                serverPart = serverPart.substring( idx + 1 );
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

        if ( optionsPart != null && optionsPart.length() > 0 ) { // _options TODO
            throw new RuntimeException( "not done" );
        }
    }

    // ---------------------------------

    public String getUsername(){
        return _username;
    }
    
    public char[] getPassword(){
        return _password;
    }

    public List<String> getHosts(){
        return _hosts;
    }
     
    public String getDatabase(){
        return _database;
    }

    public String getCollection(){
        return _collection;
    }

    public MongoOptions getOptions(){
        return _options;
    }

    public Mongo connect() 
        throws MongoException , UnknownHostException {
        // TODO caching?
        return new Mongo( this );
    }

    public DB connectDB()
        throws MongoException , UnknownHostException {
        // TODO auth
        return connect().getDB( _database );
    }

    public DB connectDB( Mongo m ){
        // TODO auth
        return m.getDB( _database );
    }

    public DBCollection connectCollection( DB db ){
        return db.getCollection( _collection );
    }

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

    @Override
    public String toString() {
        return _uri;
    }
}

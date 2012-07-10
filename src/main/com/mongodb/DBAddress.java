// DBAddress.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Represents a database address
 */
public class DBAddress extends ServerAddress {
    
    /** Creates a new address
     * Accepts as the parameter format:
     * <table border="1">
     * <tr>
     *   <td><i>name</i></td>
     *   <td>"mydb"</td>
     * </tr>
     * <tr>
     *   <td><i>&lt;host&gt;/name</i></td>
     *   <td>"127.0.0.1/mydb"</td>
     * </tr>
     * <tr>
     *   <td><i>&lt;host&gt;:&lt;port&gt;/name</i></td>
     *   <td>"127.0.0.1:8080/mydb"</td>
     * </tr>
     * </table>
     * @param urlFormat
     * @throws UnknownHostException
     */
    public DBAddress( String urlFormat )
        throws UnknownHostException {
        super( _getHostSection( urlFormat ) );

        _check( urlFormat , "urlFormat" );
        _db = _fixName( _getDBSection( urlFormat ) );
        
        _check( _host , "host" );
        _check( _db , "db" );
    }

    static String _getHostSection( String urlFormat ){
        if ( urlFormat == null )
            throw new NullPointerException( "urlFormat can't be null" );
        int idx = urlFormat.indexOf( "/" );
        if ( idx >= 0 )
            return urlFormat.substring( 0 , idx );
        return null;
    }

    static String _getDBSection( String urlFormat ){
        if ( urlFormat == null )
            throw new NullPointerException( "urlFormat can't be null" );
        int idx = urlFormat.indexOf( "/" );
        if ( idx >= 0 )
            return urlFormat.substring( idx + 1 );
        return urlFormat;
    }
    
    static String _fixName( String name ){
        name = name.replace( '.' , '-' );
        return name;
    }

    /**
     * @param other an existing <code>DBAddress</code> that gives the host and port
     * @param dbname the database to which to connect
     * @throws UnknownHostException
     */
    public DBAddress( DBAddress other , String dbname )
        throws UnknownHostException {
        this( other._host , other._port , dbname );
    }

    /**
     * @param host host name
     * @param dbname database name
     * @throws UnknownHostException
     */
    public DBAddress( String host , String dbname )
        throws UnknownHostException {
        this( host , DBPort.PORT , dbname );
    }
    
    /**
     * @param host host name
     * @param port database port
     * @param dbname database name
     * @throws UnknownHostException
     */
    public DBAddress( String host , int port , String dbname )
        throws UnknownHostException {
        super( host , port );
        _db = dbname.trim();
    }

    /**
     * @param addr host address
     * @param port database port
     * @param dbname database name
     */
    public DBAddress( InetAddress addr , int port , String dbname ){
        super( addr , port );
        _check( dbname , "name" );
        _db = dbname.trim();
    }

    static void _check( String thing , String name ){
        if ( thing == null )
            throw new NullPointerException( name + " can't be null " );
        
        thing = thing.trim();
        if ( thing.length() == 0 )
            throw new IllegalArgumentException( name + " can't be empty" );
    }

    @Override
    public int hashCode(){
        return super.hashCode() + _db.hashCode();
    }

    @Override
    public boolean equals( Object other ){
        if ( other instanceof DBAddress ){
            DBAddress a = (DBAddress)other;
            return 
                a._port == _port &&
                a._db.equals( _db ) &&
                a._host.equals( _host );
        } else if ( other instanceof ServerAddress ){
            return other.equals(this);
        }
        return false;
    }


    /**
     * creates a DBAddress pointing to a different database on the same server
     * @param name database name
     * @return
     * @throws MongoException
     */
    public DBAddress getSister( String name ){
        try {
            return new DBAddress( _host , _port , name );
        }
        catch ( UnknownHostException uh ){
            throw new MongoInternalException( "shouldn't be possible" , uh );
        }
    }
    
    /**
     * gets the database name
     * @return
     */
    public String getDBName(){
        return _db;
    }

    /**
     * gets a String representation of address as host:port/dbname.
     * @return this address
     */
    @Override
    public String toString(){
        return super.toString() + "/" + _db;
    }
    
    final String _db;
}

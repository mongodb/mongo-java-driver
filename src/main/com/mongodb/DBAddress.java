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

import java.net.*;
import java.util.*;

import com.mongodb.util.*;

/** Aquires the address of the database(s).
 */
public class DBAddress {
    
    /** Creates a new address
     * Accepts as the parameter format:
     * <table border="1">
     * <tr>
     *   <td><i>name</i></td>
     *   <td>"127.0.0.1"</td>
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
     */
    public DBAddress( String urlFormat )
        throws UnknownHostException {
        _check( urlFormat , "urlFormat" );
        
        int idx = urlFormat.indexOf( "/" );
        if ( idx < 0 ){
            _host = defaultHost();
            _port = defaultPort();
            _name = _fixName( urlFormat );
        }
        else {
            _name = _fixName( urlFormat.substring( idx + 1 ).trim() );
            urlFormat = urlFormat.substring( 0 , idx ).trim();
            idx = urlFormat.indexOf( ":" );
            if ( idx < 0 ){
                _host = urlFormat.trim();
                _port = defaultPort();
            }
            else {
                _host = urlFormat.substring( 0 , idx );
                _port = Integer.parseInt( urlFormat.substring( idx + 1 ) );
            }
        }
        
        _check( _host , "host" );
        _check( _name , "name" );
        
        _addrs = _getAddress( _host );
        _addr = _addrs[0];
    }
    
    static String _fixName( String name ){
        name = name.replace( '.' , '-' );
        return name;
    }

    /** Connects to a given database using the host/port information from an existing <code>DBAddress</code>.
     * @param other the existing <code>DBAddress</code>
     * @param name the database to which to connect
     */
    public DBAddress( DBAddress other , String name )
        throws UnknownHostException {
        this( other._host , other._port , name );
    }

    /** Connects to a database with a given name at a given host.
     * @param host host name
     * @param name database name
     */
    public DBAddress( String host , String name )
        throws UnknownHostException {
        this( host , DBPort.PORT , name );
    }
    
    /** Connects to a database with a given host, port, and name
     * @param host host name
     * @param port database port
     * @param name database name
     */
    public DBAddress( String host , int port , String name )
        throws UnknownHostException {
        _check( host , "host" );
        _check( name , "name" );
        
        _host = host.trim();
        _port = port;
        _name = name.trim();

        _addrs = _getAddress( _host );
        _addr = _addrs[0];
    }

    /** Connects to a database with a given host, port, and name
     * @param host host address
     * @param port database port
     * @param name database name
     */
    public DBAddress( InetAddress addr , int port , String name ){
        if ( addr == null )
            throw new IllegalArgumentException( "addr can't be null" );
        _check( name , "name" );
        
        _host = addr.getHostName();
        _port = port;
        _name = name.trim();
        
        _addr = addr;
        _addrs = new InetAddress[]{ addr };
    }

    static void _check( String thing , String name ){
        if ( thing == null )
            throw new NullPointerException( name + " can't be null " );
        
        thing = thing.trim();
        if ( thing.length() == 0 )
            throw new IllegalArgumentException( name + " can't be empty" );
    }

    public int hashCode(){
        return _host.hashCode() + _port + _name.hashCode();
    }

    public boolean equals( Object other ){
        if ( other instanceof DBAddress ){
            DBAddress a = (DBAddress)other;
            return 
                a._port == _port &&
                a._name.equals( _name ) &&
                a._host.equals( _host );
        }
        return false;
    }

    /** String representation of address as host:port/dbname.
     * @return this address
     */
    public String toString(){
        return _host + ":" + _port + "/" + _name;
    }

    /** Creates an new <code>InetSocketAddress</code> using this address.
     * @param the socket address
     */
    public InetSocketAddress getSocketAddress(){
        return new InetSocketAddress( _addr , _port );
    }

    /** Determines whether this address is the same as a given host.
     * @param host the address to compare
     * @return if they are the same
     */
    public boolean sameHost( String host ){
        int idx = host.indexOf( ":" );
        int port = defaultPort();
        if ( idx > 0 ){
            port = Integer.parseInt( host.substring( idx + 1 ) );
            host = host.substring( 0 , idx );
        }

        return 
            _port == port &&
            _host.equalsIgnoreCase( host );
    }

    /** Determines if the database at this address is paired.
     * @return if this address connects to a set of paired databases
     */
    boolean isPaired(){
        return _addrs.length > 1;
    }

    /** If this is the address of a paired database, returns addresses for
     * all of the databases with which it is paired.
     * @return the addresses
     * @throws RuntimeException if this address is not one of a paired database
     */
    List<DBAddress> getPairedAddresses(){
        if ( _addrs.length != 2 )
            throw new RuntimeException( "not paired.  num addressed : " + _addrs.length );
        
        List<DBAddress> l = new ArrayList<DBAddress>();
        for ( int i=0; i<_addrs.length; i++ ){
            l.add( new DBAddress( _addrs[i] , _port , _name ) );
        }
        return l;
    }

    public DBAddress getSister( String name ){
        try {
            return new DBAddress( _host , _port , name );
        }
        catch ( UnknownHostException uh ){
            throw new MongoInternalException( "shouldn't be possible" , uh );
        }
    }

    public String getHost(){
        return _host;
    }
    
    public int getPort(){
        return _port;
    }
    
    public String getDBName(){
        return _name;
    }
       
    
    final String _host;
    final int _port;
    final String _name;
    final InetAddress _addr;
    final InetAddress[] _addrs;
    
    private static InetAddress[] _getAddress( String host )
        throws UnknownHostException {

        if (host.toLowerCase().equals("localhost")) {
            return new InetAddress[] { InetAddress.getLocalHost()};
        }
        
        return InetAddress.getAllByName( host );
    }

    /** Returns the default database host.
     * @return the db_ip environmental variable, or "127.0.0.1" as a default
     */
    public static String defaultHost(){
        return Config.get().getTryEnvFirst( "db_ip" , "127.0.0.1" );
    }

    /** Returns the default port that the database runs on.
     * @return the db_port environmental variable, or 27017 as a default
     */
    public static int defaultPort(){
        return Integer.parseInt(Config.get().getTryEnvFirst("db_port", Integer.toString(DBPort.PORT)));
    }
    
}

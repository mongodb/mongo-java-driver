// ServerAddress.java

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

/**
 * mongo server address
 */
public class ServerAddress {
    
    public ServerAddress()
        throws UnknownHostException {
        this( defaultHost() , defaultPort() );
    }
    
    public ServerAddress( String host )
        throws UnknownHostException {
        this( host , defaultPort() );
    }

    public ServerAddress( String host , int port )
        throws UnknownHostException {
        if ( host == null )
            host = defaultHost();
        host = host.trim();
        if ( host.length() == 0 )
            host = defaultHost();
        
        int idx = host.indexOf( ":" );
        if ( idx > 0 ){
            if ( port != defaultPort() )
                throw new IllegalArgumentException( "can't specify port in construct and via host" );
            port = Integer.parseInt( host.substring( idx + 1 ) );
            host = host.substring( 0 , idx ).trim();
        }

        _host = host;
        _port = port;
        _all = _getAddress( _host );
        _addr = new InetSocketAddress( _all[0] , _port );
    }

    public ServerAddress( InetAddress addr ){
        this( new InetSocketAddress( addr , defaultPort() ) );
    }

    public ServerAddress( InetAddress addr , int port ){
        this( new InetSocketAddress( addr , port ) );
    }

    public ServerAddress( InetSocketAddress addr ){
        _addr = addr;
        _host = _addr.getHostName();
        _port = _addr.getPort();
        _all = null;
    }
    
    // --------
    // pairing
    // --------

    /** Determines if the database at this address is paired.
     * @return if this address connects to a set of paired databases
     */
    boolean isPaired(){
        return _all != null && _all.length > 1;
    }

    /** If this is the address of a paired database, returns addresses for
     * all of the databases with which it is paired.
     * @return the addresses
     * @throws RuntimeException if this address is not one of a paired database
     */
    List<ServerAddress> explode(){
        if ( _all == null || _all.length <= 1 )
            throw new RuntimeException( "not replica set mode.  num addresses : " + ((_all == null) ? 0 : _all.length) );
        
        List<ServerAddress> s = new ArrayList<ServerAddress>();
        for ( int i=0; i<_all.length; i++ ){
            s.add( new ServerAddress( _all[i] , _port ) );
        }
        return s;
    }

    // --------
    // equality, etc...
    // --------


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

    public boolean equals( Object other ){
        if ( other instanceof ServerAddress ){
            ServerAddress a = (ServerAddress)other;
            return 
                a._port == _port &&
                a._host.equals( _host );
        }
        if ( other instanceof InetSocketAddress ){
            return _addr.equals( other );
        }
        return false;
    }

    public int hashCode(){
        return _host.hashCode() + _port;
    }

    public String getHost(){
        return _host;
    }

    public int getPort(){
        return _port;
    }
    
    public InetSocketAddress getSocketAddress(){
        return _addr;
    }

    public String toString(){
        return _host + ":" + _port;
    }

    final String _host;
    final int _port;
    final InetSocketAddress _addr;
    final InetAddress[] _all;

    // --------
    // static helpers
    // --------

    private static InetAddress[] _getAddress( String host )
        throws UnknownHostException {

        if ( host.toLowerCase().equals("localhost") ){
            return new InetAddress[] { InetAddress.getLocalHost()};
        }
        
        return InetAddress.getAllByName( host );
    }
    
    /** Returns the default database host.
     * @return the db_ip environmental variable, or "127.0.0.1" as a default
     */
    public static String defaultHost(){
        return "127.0.0.1";
    }

    /** Returns the default port that the database runs on.
     * @return the db_port environmental variable, or 27017 as a default
     */
    public static int defaultPort(){
        return DBPort.PORT;
    }
    
}

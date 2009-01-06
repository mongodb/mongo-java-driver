// DBAddress.java

package ed.db;

import java.net.*;
import java.util.*;

import ed.util.*;

public class DBAddress {
    
    /**
     * name
     * <host>/name
     * <host>:<port>/name
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

    public DBAddress( DBAddress other , String name )
        throws UnknownHostException {
        this( other._host , other._port , name );
    }

    public DBAddress( String host , String name )
        throws UnknownHostException {
        this( host , DBPort.PORT , name );
    }
    
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

    public String toString(){
        return _host + ":" + _port + "/" + _name;
    }

    public InetSocketAddress getSocketAddress(){
        return new InetSocketAddress( _addr , _port );
    }

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

    boolean isPaired(){
        return _addrs.length > 1;
    }

    List<DBAddress> getPairedAddresses(){
        if ( _addrs.length != 2 )
            throw new RuntimeException( "not paired.  num addressed : " + _addrs.length );
        
        List<DBAddress> l = new ArrayList<DBAddress>();
        for ( int i=0; i<_addrs.length; i++ ){
            l.add( new DBAddress( _addrs[i] , _port , _name ) );
        }
        return l;
    }
    
    final String _host;
    final int _port;
    final String _name;
    final InetAddress _addr;
    final InetAddress[] _addrs;
    
    private static InetAddress[] _getAddress( String host )
        throws UnknownHostException {
        return InetAddress.getAllByName( host );
    }

    public static String defaultHost(){
        return Config.get().getTryEnvFirst( "db_ip" , "127.0.0.1" );
    }

    public static int defaultPort(){
        return Integer.parseInt(Config.get().getTryEnvFirst("db_port", Integer.toString(DBPort.PORT)));
    }
    
}

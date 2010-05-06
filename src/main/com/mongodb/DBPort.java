// DBPort.java

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

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.logging.*;

import com.mongodb.util.*;

public class DBPort {
    
    public static final int PORT = 27017;
    static final boolean USE_NAGLE = false;
    
    static final long CONN_RETRY_TIME_MS = 15000;

    public DBPort( InetSocketAddress addr )
        throws IOException {
        this( addr , null , new MongoOptions() );
    }
    
    DBPort( InetSocketAddress addr  , DBPortPool pool , MongoOptions options )
        throws IOException {
        _options = options;
        _addr = addr;
        _pool = pool;

        _hashCode = _addr.hashCode();

        _logger = Logger.getLogger( _rootLogger.getName() + "." + addr.toString() );
    }

    /**
     * @param response will get wiped
     */
    DBMessage call( DBMessage msg , ByteDecoder decoder )
        throws IOException {
        return go( msg , decoder );
    }
    
    void say( DBMessage msg )
        throws IOException {
        go( msg , null );
    }

    private synchronized DBMessage go( DBMessage msg , ByteDecoder decoder )
        throws IOException {
        
        if ( _sock == null )
            _open();
        
        {
            ByteBuffer out = msg.prepare();
            while ( out.remaining() > 0 )
                _sock.write( out );
        }            
        
        if ( _pool != null )
            _pool._everWorked = true;

        if ( decoder == null )
            return null;

        ByteBuffer response = decoder._buf;
        
        if ( response.position() != 0 )
            throw new IllegalArgumentException();

        int read = 0;
        while ( read < DBMessage.HEADER_LENGTH )
            read += _read( response );

        int len = response.getInt(0);
        if ( len <= DBMessage.HEADER_LENGTH )
            throw new IllegalArgumentException( "db sent invalid length: " + len );

        if ( len > response.capacity() )
            throw new IllegalArgumentException( "db message size is too big (" + len + ") " +
                                                "max is (" + response.capacity() + ")" );
        
        response.limit( len );
        while ( read < len )
            read += _read( response );
        
        if ( read != len )
            throw new RuntimeException( "something is wrong" );

        response.flip();
        return new DBMessage( response );
    }

    synchronized void say( OutMessage out )
        throws IOException {
        
        if ( _sock == null )
            _open();
        
        out.pipe( _out );
    }

    public synchronized void ensureOpen()
        throws IOException {
        
        if ( _sock != null )
            return;
        
        _open();
    }

    void _open()
        throws IOException {
        
        long sleepTime = 100;

        final long start = System.currentTimeMillis();
        while ( true ){
            
            IOException lastError = null;

            try {
                _sock = SocketChannel.open();
                _socket = _sock.socket();
                _socket.connect( _addr , _options.connectTimeout );
                
                _socket.setTcpNoDelay( ! USE_NAGLE );
                _socket.setSoTimeout( _options.socketTimeout );
                _in = _socket.getInputStream();
                _out = _socket.getOutputStream();
                return;
            }
            catch ( IOException ioe ){
//  TODO  - erh to fix                lastError = new IOException( "couldn't connect to [" + _addr + "] bc:" + lastError , lastError );
                lastError = new IOException( "couldn't connect to [" + _addr + "] bc:" + ioe );
                _logger.log( Level.INFO , "connect fail to : " + _addr , ioe );
            }
            
            if ( ! _options.autoConnectRetry || ( _pool != null && ! _pool._everWorked ) )
                throw lastError;
            
            long sleptSoFar = System.currentTimeMillis() - start;

            if ( sleptSoFar >= CONN_RETRY_TIME_MS )
                throw lastError;
            
            if ( sleepTime + sleptSoFar > CONN_RETRY_TIME_MS )
                sleepTime = CONN_RETRY_TIME_MS - sleptSoFar;

            _logger.severe( "going to sleep and retry.  total sleep time after = " + ( sleptSoFar + sleptSoFar ) + "ms  this time:" + sleepTime + "ms" );
            ThreadUtil.sleep( sleepTime );
            sleepTime *= 2;
            
        }
        
    }

    public int hashCode(){
        return _hashCode;
    }
    
    public String host(){
        return _addr.toString();
    }
    
    public String toString(){
        return "{DBPort  " + host() + "}";
    }
    
    protected void finalize(){
        if ( _sock != null ){
            try {
                _sock.close();
            }
            catch ( Exception e ){
                // don't care
            }
            
            _in = null;
            _out = null;
            _socket = null;
            _sock = null;            
        }

    }

    void checkAuth( DB db ){
        if ( db._username == null )
            return;
        if ( _authed.containsKey( db ) )
            return;
        
        if ( _inauth )
            return;
        
        _inauth = true;
        try {
            if ( db.reauth() ){
                _authed.put( db , true );
                return;
            }
        }
        finally {
            _inauth = false;
        }

        throw new MongoInternalException( "can't reauth!" );
    }

    private int _read( ByteBuffer buf )
        throws IOException {
        int x = _in.read( buf.array() , buf.position() , buf.remaining() );
        if ( x < 0 )
            throw new IOException( "connection to server closed unexpectedly" );
        buf.position( buf.position() + x );
        return x;
    }
    
    
    final int _hashCode;
    final InetSocketAddress _addr;
    final DBPortPool _pool;
    final MongoOptions _options;
    final Logger _logger;
    
    private SocketChannel _sock;
    private Socket _socket;
    private InputStream _in;
    private OutputStream _out;

    private boolean _inauth = false;
    private Map<DB,Boolean> _authed = Collections.synchronizedMap( new WeakHashMap<DB,Boolean>() );

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );
}

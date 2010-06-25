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
    Response call( OutMessage msg , DBCollection coll )
        throws IOException {
        return go( msg , coll );
    }
    
    void say( OutMessage msg )
        throws IOException {
        go( msg , null );
    }
    
    private synchronized Response go( OutMessage msg , DBCollection coll )
        throws IOException {
    
        _calls++;
    
        if ( _socket == null )
            _open();
        
        try {
            msg.prepare();
            msg.pipe( _out );
            
            if ( _pool != null )
                _pool._everWorked = true;
            
            if ( coll == null )
                return null;
            
            return new Response( coll , _in );
        }
        catch ( IOException ioe ){
            close();
            throw ioe;
        }
    }

    synchronized BasicDBObject getLastError( DB db ){

        OutMessage msg = OutMessage.query( 0 , db.getName() + ".$cmd" , 0 , -1 , new BasicDBObject( "getlasterror" , 1 ) , null );
        
        try {
            Response res = go( msg , db.getCollection( "$cmd" ) );
            if ( res.size() != 1 )
                throw new MongoInternalException( "something is wrong.  size:" + res.size() );
            return (BasicDBObject)res.get(0);
        }
        catch ( IOException ioe ){
            throw new MongoInternalException( "getlasterror failed: " + ioe.toString() , ioe );
        }
    }
    
    synchronized BasicDBObject tryGetLastError( DB db , long last ){
        if ( last != _calls )
            return null;
        
        return getLastError( db );
    }

    public synchronized void ensureOpen()
        throws IOException {
        
        if ( _socket != null )
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
                _socket = new Socket();
                _socket.connect( _addr , _options.connectTimeout );
                
                _socket.setTcpNoDelay( ! USE_NAGLE );
                _socket.setSoTimeout( _options.socketTimeout );
                _in = new BufferedInputStream( _socket.getInputStream() );
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
        close();
    }

    protected void close(){
        if ( _socket == null )
            return;
        
        try {
            _socket.close();
        }
        catch ( Exception e ){
            // don't care
        }
        
        _in = null;
        _out = null;
        _socket = null;
    }

    void checkAuth( DB db ){
        if ( db._username == null )
            return;
        if ( _authed.containsKey( db ) )
            return;
        
        if ( _inauth )
            return;
        
        _inauth = true;
        OutMessage real = OutMessage.TL.get();
        OutMessage.TL.set( new OutMessage() );
        try {
            if ( db.reauth() ){
                _authed.put( db , true );
                return;
            }
        }
        finally {
            _inauth = false;
            OutMessage.TL.set( real );
        }

        throw new MongoInternalException( "can't reauth!" );
    }

    final int _hashCode;
    final InetSocketAddress _addr;
    final DBPortPool _pool;
    final MongoOptions _options;
    final Logger _logger;
    
    private Socket _socket;
    private InputStream _in;
    private OutputStream _out;
    
    private boolean _inauth = false;
    private Map<DB,Boolean> _authed = Collections.synchronizedMap( new WeakHashMap<DB,Boolean>() );
    int _lastThread;
    long _calls = 0;

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );
}

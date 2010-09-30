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
import java.util.*;
import java.util.logging.*;

import org.bson.*;
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

        if ( _processingResponse ){
            if ( coll == null ){
                // this could be a pipeline and should be safe
            }
            else {
                // this could cause issues since we're reading data off the wire
                throw new IllegalStateException( "DBPort.go called and expecting a response while processing another response" );
            }
        }

        _calls++;
    
        if ( _socket == null )
            _open();
        
        if ( _out == null )
            throw new IllegalStateException( "_out shouldn't be null" );

        try {
            msg.prepare();
            msg.pipe( _out );
            
            if ( _pool != null )
                _pool._everWorked = true;
            
            if ( coll == null )
                return null;
            
            _processingResponse = true;
            return new Response( coll , _in , _decoder);
        }
        catch ( IOException ioe ){
            close();
            throw ioe;
        }
        finally {
            _processingResponse = false;
        }
    }

    synchronized CommandResult getLastError( DB db , WriteConcern concern){
	DBApiLayer dbAL = (DBApiLayer) db;
	return runCommand( dbAL , concern.getCommand() );
    }

    synchronized CommandResult runCommand( DB db , DBObject cmd ) {
        OutMessage msg = OutMessage.query( db._mongo , 0 , db.getName() + ".$cmd" , 0 , -1 , cmd , null );
        
        try {
            Response res = go( msg , db.getCollection( "$cmd" ) );
            if ( res.size() != 1 )
                throw new MongoInternalException( "something is wrong.  size:" + res.size() );
            return (CommandResult)res.get(0);
        }
        catch ( IOException ioe ){
            throw new MongoInternalException( "cmd failed: " + ioe.toString() , ioe );
        }
        
    }

    synchronized CommandResult tryGetLastError( DB db , long last, WriteConcern concern){
        if ( last != _calls )
            return null;
        
        return getLastError( db , concern );
    }

    public synchronized void ensureOpen()
        throws IOException {
        
        if ( _socket != null )
            return;
        
        _open();
    }

    boolean _open()
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
                return true;
            }
            catch ( IOException ioe ){
                lastError = new IOException( "couldn't connect to [" + _addr + "] bc:" + ioe );
                _logger.log( Level.INFO , "connect fail to : " + _addr , ioe );
                close();
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
        _authed.clear();
                
        if ( _socket != null ){
            try {
                _socket.close();
            }
            catch ( Exception e ){
                // don't care
            }
        }
        
        _in = null;
        _out = null;
        _socket = null;
    }
    
    void checkAuth( DB db ){
        if ( db._username == null ){
            if ( db._name.equals( "admin" ) )
                return;
            checkAuth( db._mongo.getDB( "admin" ) );
            return;
        }
        if ( _authed.containsKey( db ) )
            return;
        
        CommandResult res = runCommand( db , new BasicDBObject( "getnonce" , 1 ) );
        res.throwOnError();
        
        DBObject temp = db._authCommand( res.getString( "nonce" ) );
        
        res = runCommand( db , temp );

        if ( ! res.ok() )
            throw new MongoInternalException( "couldn't re-auth" );
        _authed.put( db , true );
    }
    
    final int _hashCode;
    final InetSocketAddress _addr;
    final DBPortPool _pool;
    final MongoOptions _options;
    final Logger _logger;
    final BSONDecoder _decoder = new BSONDecoder();
    
    private Socket _socket;
    private InputStream _in;
    private OutputStream _out;

    private boolean _processingResponse;

    private Map<DB,Boolean> _authed = Collections.synchronizedMap( new WeakHashMap<DB,Boolean>() );
    int _lastThread;
    long _calls = 0;

    private static Logger _rootLogger = Logger.getLogger( "com.mongodb.port" );
}

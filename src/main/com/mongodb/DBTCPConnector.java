// DBTCPConnector.java

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

class DBTCPConnector implements DBConnector {

    static Logger _logger = Logger.getLogger( Bytes.LOGGER.getName() + ".tcp" );
    static Logger _createLogger = Logger.getLogger( _logger.getName() + ".connect" );

    public DBTCPConnector( Mongo m , DBAddress addr )
        throws MongoException {
        _mongo = m;
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( addr );

        _createLogger.info( addr.toString() );

        if ( addr.isPaired() ){
            _allHosts = new ArrayList<DBAddress>( addr.getPairedAddresses() );
            _validatePairs( _allHosts );
            _pickInitial();
            _createLogger.info( "switch to paired mode : " + _allHosts + " -> " + _curAddress  );
        }
        else {
            _set( addr );
            _allHosts = null;
        }

    }

    public DBTCPConnector( Mongo m , DBAddress ... all )
        throws MongoException {
        this( m , Arrays.asList( all ) );
    }

    public DBTCPConnector( Mongo m , List<DBAddress> all )
        throws MongoException {
        _mongo = m;
        _portHolder = new DBPortPool.Holder( m._options );
        _checkAddress( all );

        _validatePairs( all );

        _allHosts = new ArrayList<DBAddress>( all ); // make a copy so it can't be modified
        _pickInitial();

        _createLogger.info( all  + " -> " + _curAddress );
    }

    private void _validatePairs( List<DBAddress> all ){

        final String name = all.get(0)._name;
        for ( int i=1; i<all.size(); i++ ){
            if ( ! all.get(i)._name.equals( name ) )
                throw new IllegalArgumentException( " names don't match [" + all.get(i)._name + "] != [" + name + "]" );
        }

    }

    private static DBAddress _checkAddress( DBAddress addr ){
        if ( addr == null )
            throw new NullPointerException( "address can't be null" );
        return addr;
    }

    private static DBAddress _checkAddress( List<DBAddress> addrs ){
        if ( addrs == null )
            throw new NullPointerException( "addresses can't be null" );
        if ( addrs.size() == 0 )
            throw new IllegalArgumentException( "need to specify at least 1 address" );
        return addrs.get(0);
    }

    /**
     * Start a "request".
     *
     * A "request" is a group of operations in which order matters. Examples
     * include inserting a document and then performing a query which expects
     * that document to have been inserted, or performing an operation and
     * then using com.mongodb.Mongo.getLastError to perform error-checking
     * on that operation. When a thread performs operations in a "request", all
     * operations will be performed on the same socket, so they will be
     * correctly ordered.
     */
    public void requestStart(){
        _threadPort.get().requestStart();
    }

    /**
     * End the current "request", if this thread is in one.
     *
     * By ending a request when it is safe to do so the built-in connection-
     * pool is allowed to reassign requests to different sockets in order to
     * more effectively balance load. See requestStart for more information.
     */
    public void requestDone(){
        _threadPort.get().requestDone();
    }

    public void requestEnsureConnection(){
        _threadPort.get().requestEnsureConnection();
    }

    public void say( int op , ByteBuffer buf , DB.WriteConcern concern )
        throws MongoException {
        MyPort mp = _threadPort.get();
        DBPort port = mp.get( true );

        try {
            port.say( new DBMessage( op , buf ) );
            if ( concern == DB.WriteConcern.STRICT ){
                DBObject e = _mongo.getDB( "admin" ).getLastError();
                Object foo = e.get( "err" );
                if ( foo != null ){
                    String s = foo.toString();
                    if ( s.startsWith( "E11000" ) ||
                         s.startsWith( "E11001" ) )
                        throw new MongoException.DuplicateKey( s );
                    throw new MongoException( s );
                }
            }
            mp.done( port );
        }
        catch ( IOException ioe ){
            mp.error( ioe );
            _error();
            if ( concern == DB.WriteConcern.NONE )
                return;
            throw new MongoException.Network( "can't say something" , ioe );
        }
    }
    
    public int call( int op , ByteBuffer out , ByteBuffer in )
        throws MongoException {
        return _call( op , out , in , 2 );
    }

    private int _call( int op , ByteBuffer out , ByteBuffer in , int retries )
        throws MongoException {
        MyPort mp = _threadPort.get();
        DBPort port = mp.get( false );

        try {
            DBMessage a = new DBMessage( op , out );
            DBMessage b = port.call( a , in );
            mp.done( port );

            String err = _getError( in );

            if ( err != null ){
                if ( "not master".equals( err ) ){
                    _pickCurrent();
                    if ( retries <= 0 )
                        throw new MongoException( "not talking to master and retries used up" );
                    in.position( 0 );

                    return _call( op , out , in , retries -1 );
                }
            }

            return b.dataLen();
        }
        catch ( IOException ioe ){
            mp.error( ioe );
            if ( _error() && retries > 0 ){
                in.position( 0 );
                return _call( op , out , in , retries - 1 );
            }
            throw new MongoException.Network( "can't call something" , ioe );
        }
    }

    public DBAddress getAddress(){
        return _curAddress;
    }

    public String getConnectPoint(){
        return _curAddress.toString();
    }

    boolean _error()
        throws MongoException {
        if ( _allHosts != null )
            _pickCurrent();
        return true;
    }

    String _getError( ByteBuffer buf ){
        DBApiLayer.QueryHeader header = new DBApiLayer.QueryHeader( buf , 0 );
        if ( header._num != 1 )
            return null;

        DBObject obj = new RawDBObject( buf , header.headerSize() );
        Object err = obj.get( "$err" );
        if ( err == null )
            return null;

        return err.toString();
    }

    class MyPort {

        DBPort get( boolean keep ){
            _internalStack++;

            if ( _internalStack > 1 ){
                if ( _last == null ){
                    System.err.println( "_internalStack > 1 and _last is null!" );
                }
                else {
                    return _last;
                }
            }
            if ( _port != null )
                return _port;
            
            DBPort p = _curPortPool.get();
            if ( keep && _inRequest )
                _port = p;
            
            _last = p;
            return p;
        }
        
        void done( DBPort p ){

            _internalStack--;

            if ( p != _port && _internalStack <=0 )
                _curPortPool.done( p );

            if ( _internalStack < 0 ){
                System.err.println( "_internalStack < 0 : " + _internalStack );
                _internalStack = 0;
            }
        }

        void error( Exception e ){
            _port = null;
            _curPortPool.gotError( e );

            _internalStack = 0;
            _last = null;
        }
        
        void requestEnsureConnection(){
            if ( ! _inRequest )
                return;

            if ( _port != null )
                return;

            _port = _curPortPool.get();
        }

        void requestStart(){
            _inRequest = true;
            if ( _port != null ){
                _port = null;
                System.err.println( "ERROR.  somehow _port was not null at requestStart" );
            }
        }

        void requestDone(){
            if ( _port != null )
                _curPortPool.done( _port );
            _port = null;
            _inRequest = false;
            if ( _internalStack > 0 ){
                System.err.println( "_internalStack in requestDone should be 0" );
                _internalStack = 0;
            }
        }
        
        int _internalStack = 0;

        DBPort _port;
        DBPort _last;
        boolean _inRequest;
    }
    
    private void _pickInitial()
        throws MongoException {
        assert( _curAddress == null );

        // we need to just get a server to query for ismaster
        _pickCurrent();

        try {
            System.out.println( _curAddress );
            DBCollection collection = _mongo.getDB( "admin" ).getCollection( "$cmd" );
            Iterator<DBObject> i = collection.find( _isMaster , null , 0 , 1 );
            if ( i == null || ! i.hasNext() )
                throw new MongoException( "no result for ismaster query?" );
            DBObject res = i.next();
            if ( i.hasNext() )
                throw new MongoException( "what's going on" );

            int ismaster = ((Number)res.get( "ismaster" )).intValue();
            if ( 1 == ismaster )
                return;

            if ( res.get( "remote" ) == null )
                throw new MongoException( "remote not sent back!" );

            String remote = res.get( "remote" ).toString();
            synchronized ( _allHosts ){
                for ( DBAddress a : _allHosts ){
                    if ( ! a.sameHost( remote ) )
                        continue;
                    System.out.println( "remote [" + remote + "] -> [" + a + "]" );
                    _set( a );
                    return;
                }
            }
        }
        catch ( Exception e ){
            _logger.log( Level.SEVERE , "can't pick initial master, using random one" , e );
        }
    }

    private void _pickCurrent()
        throws MongoException {
        if ( _allHosts == null )
            throw new MongoException( "got master/slave issue but not in master/slave mode on the client side" );

        synchronized ( _allHosts ){
            Collections.shuffle( _allHosts );
            for ( int i=0; i<_allHosts.size(); i++ ){
                DBAddress a = _allHosts.get( i );
                if ( a == _curAddress )
                    continue;

                if ( _curAddress != null )
                    _logger.info( "switching from [" + _curAddress + "] to [" + a + "]" );

                _set( a );
                return;
            }
        }

        throw new MongoException( "couldn't find a new host to swtich too" );
    }

    private boolean _set( DBAddress addr ){
        if ( _curAddress == addr )
            return false;
        _curAddress = addr;
        _curPortPool = _portHolder.get( _curAddress.getSocketAddress() );
        return true;
    }

    public String debugString(){
        StringBuilder buf = new StringBuilder( "DBTCPConnector: " );
        if ( _allHosts != null )
            buf.append( "paired : " ).append( _allHosts );
        else
            buf.append( _curAddress ).append( " " ).append( _curAddress._addr );

        return buf.toString();
    }

    final Mongo _mongo;
    private DBAddress _curAddress;
    private DBPortPool _curPortPool;
    private DBPortPool.Holder _portHolder;
    private final List<DBAddress> _allHosts;

    private final ThreadLocal<MyPort> _threadPort = new ThreadLocal<MyPort>(){
        protected MyPort initialValue(){
            return new MyPort();
        }
    };

    private final static DBObject _isMaster = BasicDBObjectBuilder.start().add( "ismaster" , 1 ).get();

}

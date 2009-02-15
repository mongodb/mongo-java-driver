// DBTCP.java

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

public class DBTCP extends DBMessageLayer {

    static Logger _logger = Logger.getLogger( "DBTCP" );
    static Logger _createLogger = Logger.getLogger( _logger.getName() + ".connect" );
    
    public DBTCP( DBAddress addr ){
        super( _checkAddress( addr )._name );

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
    
    public DBTCP( List<DBAddress> all ){
        super( _checkAddress( all )._name );

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

    public void requestStart(){
        _threadPort.get().requestStart();
    }
    
    public void requestDone(){
        _threadPort.get().requestDone();
    }
    
    public void requestEnsureConnection(){
        _threadPort.get().requestEnsureConnection();
    }

    protected void say( int op , ByteBuffer buf ){
        MyPort mp = _threadPort.get();
        DBPort port = mp.get( true );
                
        try {
            port.say( new DBMessage( op , buf ) );
            mp.done( port );
        }
        catch ( Exception ioe ){
            mp.error( ioe );
            _error();
            throw new RuntimeException( "can't say something" , ioe );
        }
    }
    
    protected int call( int op , ByteBuffer out , ByteBuffer in ){
        return _call( op , out , in , 2 );
    }
    
    private int _call( int op , ByteBuffer out , ByteBuffer in , int retries ){
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
                        throw new RuntimeException( "not talking to master and retries used up" );
                    in.position( 0 );
                    
                    return _call( op , out , in , retries -1 );
                }
            }

            return b.dataLen();
        }
        catch ( Exception ioe ){
            mp.error( ioe );
            if ( _error() && retries > 0 ){
                in.position( 0 );
                return _call( op , out , in , retries - 1 );
            }
            throw new RuntimeException( "can't call something" , ioe );
        }
    }
    
    public DBAddress getAddress(){
        return _curAddress;
    }
    
    public String getConnectPoint(){
        return _curAddress.toString();
    }

    boolean _error(){
        if ( _allHosts != null )
            _pickCurrent();
        return true;
    }

    String _getError( ByteBuffer buf ){
        QueryHeader header = new QueryHeader( buf , 0 );
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
            if ( _port != null )
                return _port;
            
            DBPort p = _curPortPool.get();
            if ( keep && _inRequest )
                _port = p;
            return p;
        }
        
        void done( DBPort p ){
            if ( p != _port )
                _curPortPool.done( p );
        }

        void error( Exception e ){
            _port = null;
            _curPortPool.gotError( e );
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
        }

        DBPort _port;
        boolean _inRequest;
    }

    private void _pickInitial(){
        assert( _curAddress == null );
        
        // we need to just get a server to query for ismaster
        _pickCurrent();
        
        try {
            System.out.println( _curAddress );
            DBCollection collection = getCollection( "$cmd" );
            Iterator<DBObject> i = collection.find( _isMaster , null , 0 , 1 );
            if ( i == null || ! i.hasNext() )
                throw new RuntimeException( "no result for ismaster query?" );
            DBObject res = i.next();
            if ( i.hasNext() )
                throw new RuntimeException( "what's going on" );
            
            int ismaster = ((Number)res.get( "ismaster" )).intValue();
            if ( 1 == ismaster )
                return;
            
            if ( res.get( "remote" ) == null )
                throw new RuntimeException( "remote not sent back!" );
            
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
    
    private void _pickCurrent(){
        if ( _allHosts == null )
            throw new RuntimeException( "got master/slave issue but not in master/slave mode on the client side" );
        
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
        
        throw new RuntimeException( "couldn't find a new host to swtich too" );
    }

    private boolean _set( DBAddress addr ){
        if ( _curAddress == addr )
            return false;
        _curAddress = addr;
        _curPortPool = DBPortPool.get( _curAddress );
        return true;
    }

    public String debugString(){
        StringBuilder buf = new StringBuilder( _root );
        buf.append( " DBTCP: " );
        if ( _allHosts != null )
            buf.append( "paired : " ).append( _allHosts );
        else
            buf.append( _curAddress ).append( " " ).append( _curAddress._addr );

        return buf.toString();
    }

    private DBAddress _curAddress;
    private DBPortPool _curPortPool;
    private final List<DBAddress> _allHosts;

    private final ThreadLocal<MyPort> _threadPort = new ThreadLocal<MyPort>(){
        protected MyPort initialValue(){
            return new MyPort();
        }
    };
        
    private final static DBObject _isMaster = BasicDBObjectBuilder.start().add( "ismaster" , 1 ).get();

}

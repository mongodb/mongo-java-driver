// DBPortPool.java

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

import com.mongodb.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

class DBPortPool extends SimplePool<DBPort> {

    static class Holder {
        
        Holder( MongoOptions options ){
            _options = options;
        }
        
        DBPortPool get( InetSocketAddress addr ){
            
            DBPortPool p = _pools.get( addr );
            
            if (p != null) 
                return p;
            
            synchronized (_pools) {
                p = _pools.get( addr );
                if (p != null) {
                    return p;
                }
                
                p = new DBPortPool( addr , _options );
                _pools.put( addr , p);
            }
            
            return p;
        }

        void close(){
            synchronized ( _pools ){
                for ( DBPortPool p : _pools.values() ){
                    p.close();
                }
            }
        }
        
        final MongoOptions _options;
        final Map<InetSocketAddress,DBPortPool> _pools = Collections.synchronizedMap( new HashMap<InetSocketAddress,DBPortPool>() );
    }

    // ----
    
    public static class NoMoreConnection extends MongoInternalException {
	NoMoreConnection(){
	    super( "No more DB Connections" );
	}
    }

    // ----
    
    DBPortPool( InetSocketAddress addr , MongoOptions options ){
        super( "DBPortPool-" + addr.toString() , options.connectionsPerHost , options.connectionsPerHost );
        _options = options;
        _addr = addr;
	_waitingSem = new Semaphore( _options.connectionsPerHost * _options.threadsAllowedToBlockForConnectionMultiplier );
    }

    protected long memSize( DBPort p ){
        return 0;
    }
    
    public DBPort get(){
	DBPort port = null;
	if ( ! _waitingSem.tryAcquire() )
	    throw new NoMoreConnection();

	try {
	    port = get( _options.maxWaitTime );
	}
	finally {
	    _waitingSem.release();
	}

	if ( port == null )
	    throw new NoMoreConnection();
	
	return port;
    }

    void gotError( Exception e ){
        if ( e instanceof java.nio.channels.ClosedByInterruptException || 
             e instanceof InterruptedException ){
            // this is probably a request that is taking too long
            // so usually doesn't mean there is a real db problem
            return;
        }
        
        if ( e instanceof java.net.SocketTimeoutException && _options.socketTimeout > 0 ){
            // we don't want to clear the port pool for 1 connection timing out
            return;
        }
        Bytes.LOGGER.log( Level.INFO , "emptying DBPortPool b/c of error" , e );
        clear();
    }

    void close(){
        clear();
    }

    public boolean ok( DBPort t ){
        return _addr.equals( t._addr );
    }
    
    protected DBPort createNew()
        throws MongoInternalException{
        try {
            return new DBPort( _addr , this , _options );
        }
        catch ( IOException ioe ){
            throw new MongoInternalException( "can't create port to:" + _addr , ioe );
        }
    }

    final MongoOptions _options;
    final private Semaphore _waitingSem;
    final InetSocketAddress _addr;
    boolean _everWorked = false;
}

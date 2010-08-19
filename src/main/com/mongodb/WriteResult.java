// WriteResult.java

package com.mongodb;


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


/**
 * This class lets you access the results of the previous write.
 * if you have STRICT mode on, this just stores the result of that getLastError call
 * if you don't, then this will actually to the getlasterror call.  
 * if another op has been done on this connection in the interim, this will fail
 */
public class WriteResult {
    
    WriteResult( CommandResult o , WriteConcern concern ){
        _lastErrorResult = o;
        _lastConcern = concern;
        _lazy = false;
        _port = null;
        _db = null;
    }
    
    WriteResult( DB db , DBPort p , WriteConcern concern ){
        _db = db;
        _port = p;
        _lastCall = p._calls;
        _lastConcern = concern;
        _lazy = true;
    }

    /** @return the last result from getLastError()*/
    public CommandResult getCachedLastError(){
    	return _lastErrorResult;
    	
    }
    /** @return the last {@link WriteConcern} used when calling getLastError() */
    public WriteConcern getLastConcern(){
    	return _lastConcern;
    	
    }
    
    /**
     * <p>Calling this will either return the cache result if getLastError has been called,
     * or execute a new getLastError command on the sever.</p>
     * @throws MongoInternalException if the connection has been used since the last write operation.
     * @return {@link CommandResult} from the last write operation.
     */
    public synchronized CommandResult getLastError(){
	return getLastError(null);

    }

    public synchronized CommandResult getLastError(WriteConcern concern){
	//if the concern hasn't changed and it is cached.
        if ( _lastErrorResult != null )
		if ( ( _lastConcern != null && _lastConcern.equals( concern ) ) || ( concern != null && concern.equals( _lastConcern ) ) )
			return _lastErrorResult;
        
        if ( _port != null ){
            _lastErrorResult = _port.tryGetLastError( _db , _lastCall , (concern == null) ? new WriteConcern() : concern  );
            _lastConcern = concern;
        }
        
        if ( _lastErrorResult == null )
            throw new IllegalStateException( "The connection has been used since the last call, can't call getLastError anymore" );
        
        _lastCall++;
        return _lastErrorResult;
    }


    public String getError(){
        Object foo = getField( "err" );
        if ( foo == null )
            return null;
        return foo.toString();
    }
    
    public int getN(){
        return getLastError().getInt( "n" );
    }
    
    public Object getField( String name ){
        return getLastError().get( name );
    }

    public boolean isLazy(){
        return _lazy;
    }

    public String toString(){
        return getLastError().toString();
    }

    private long _lastCall;
    private WriteConcern _lastConcern;
    private CommandResult _lastErrorResult;
    final private DB _db;
    final private DBPort _port;
    final private boolean _lazy;
}

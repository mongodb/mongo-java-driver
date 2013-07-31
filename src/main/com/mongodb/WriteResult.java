// WriteResult.java

package com.mongodb;

import java.io.IOException;


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
 * if you don't, then this will actually do the getlasterror call.
 * if another operation has been done on this connection in the interim, calls will fail
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
        _lastCall = p._calls.get();
        _lastConcern = concern;
        _lazy = true;
    }

    /**
     * Gets the last result from getLastError()
     * @return
     */
    public CommandResult getCachedLastError(){
    	return _lastErrorResult;
    	
    }

    /** 
     * Gets the last {@link WriteConcern} used when calling getLastError()
     * @return
     */
    public WriteConcern getLastConcern(){
    	return _lastConcern;
    	
    }
    
    /**
     * calls {@link WriteResult#getLastError(com.mongodb.WriteConcern)} with concern=null
     * @return
     * @throws MongoException
     */
    public synchronized CommandResult getLastError(){
    	return getLastError(null);
    }

    /**
     * This method does following:
     * - returns the existing CommandResult if concern is null or less strict than the concern it was obtained with
     * - otherwise attempts to obtain a CommandResult by calling getLastError with the concern
     * @param concern the concern
     * @return
     * @throws MongoException
     * @deprecated Please invoke write operation with appropriate {@code WriteConcern}
     *             and then use {@link #getLastError()} method.
     */
    @Deprecated
    public synchronized CommandResult getLastError(WriteConcern concern){
        if ( _lastErrorResult != null ) {
            // do we have a satisfying concern?
            if ( concern == null || ( _lastConcern != null && _lastConcern.getW() >= concern.getW() ) )
                return _lastErrorResult;
        }

        // here we dont have a satisfying result
        if ( _port != null ){
            try {
                _lastErrorResult = _port.tryGetLastError( _db , _lastCall , (concern == null) ? new WriteConcern() : concern  );
            } catch ( IOException ioe ){
                throw new MongoException.Network( ioe.getMessage() , ioe );
            }

            if (_lastErrorResult == null)
                throw new IllegalStateException( "The connection may have been used since this write, cannot obtain a result" );
            _lastConcern = concern;
            _lastCall++;
        } else {
            // this means we dont have satisfying result and cant get new one
            throw new IllegalStateException( "Don't have a port to obtain a write result, and existing one is not good enough." );
        }

        return _lastErrorResult;
    }


    /**
     * Gets the error String ("err" field)
     * @return
     * @throws MongoException
     */
    public String getError(){
        Object foo = getField( "err" );
        if ( foo == null )
            return null;
        return foo.toString();
    }
    
    /**
     * Gets the "n" field, which contains the number of documents
     * affected in the write operation.
     * @return
     * @throws MongoException
     */
    public int getN(){
        return getLastError().getInt( "n" );
    }
    
    /**
     * Gets a field
     * @param name field name
     * @return
     * @throws MongoException
     */
    public Object getField( String name ){
        return getLastError().get( name );
    }

    /**
     * Returns whether or not the result is lazy, meaning that getLastError was not called automatically
     * @return
     */
    public boolean isLazy(){
        return _lazy;
    }

    @Override
    public String toString(){
        CommandResult res = getCachedLastError();
        if (res != null)
            return res.toString();
        return "N/A";
    }

    private long _lastCall;
    private WriteConcern _lastConcern;
    private CommandResult _lastErrorResult;
    final private DB _db;
    final private DBPort _port;
    final private boolean _lazy;
}

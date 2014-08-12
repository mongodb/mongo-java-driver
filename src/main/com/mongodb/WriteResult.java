/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// WriteResult.java

package com.mongodb;

import java.io.IOException;


/**
 * This class lets you access the results of the previous write.
 * if you have STRICT mode on, this just stores the result of that getLastError call
 * if you don't, then this will actually do the getlasterror call.
 * if another operation has been done on this connection in the interim, calls will fail
 */
@SuppressWarnings("deprecation")
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
        _lastCall = p.getUsageCount();
        _lastConcern = concern;
        _lazy = true;
    }

    /**
     * Gets the last result from getLastError().
     *
     * @return the result of the write operation
     * @deprecated Use the appropriate {@code WriteConcern} and rely on the write operation to throw an exception on failure.  For
     * successful writes, use the helper methods to retrieve specific values from the write response.
     * @see #getN()
     * @see #getUpsertedId()
     * @see #isUpdateOfExisting()
     */
    @Deprecated
    public CommandResult getCachedLastError(){
    	return _lastErrorResult;
    	
    }

    /** 
     * Gets the last {@link WriteConcern} used when calling getLastError().
     *
     * @return the write concern that was applied to the write operation
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public WriteConcern getLastConcern(){
    	return _lastConcern;
    	
    }
    
    /**
     * Calls {@link WriteResult#getLastError(com.mongodb.WriteConcern)} with a null write concern.
     *
     * @return the response to the write operation
     * @throws MongoException
     * @deprecated Use the appropriate {@code WriteConcern} and allow the write operation to throw an exception on failure.  For
     * successful writes, use the helper methods to retrieve specific values from the write response.
     * @see #getN()
     * @see #getUpsertedId()
     * @see #isUpdateOfExisting()
     */
    @Deprecated
    public synchronized CommandResult getLastError(){
    	return getLastError(null);
    }

    /**
     * This method does following:
     * - returns the existing CommandResult if concern is null or less strict than the concern it was obtained with
     * - otherwise attempts to obtain a CommandResult by calling getLastError with the concern
     * @param concern the concern
     * @return the response to the write operation
     * @throws MongoException
     * @deprecated Use the appropriate {@code WriteConcern} and rely on the write operation to throw an
     * exception on failure.  For successful writes, use the helper methods to retrieve specific values from the write response.
     * @see #getN()
     * @see #getUpsertedId()
     * @see #isUpdateOfExisting()
     */
    @Deprecated
    public synchronized CommandResult getLastError(WriteConcern concern){
        if ( _lastErrorResult != null ) {
            // do we have a satisfying concern?
            if ( concern == null || ( _lastConcern != null && _lastConcern.getW() >= concern.getW() ) )
                return _lastErrorResult;
        }

        // here we don't have a satisfying result
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
            // this means we don't have satisfying result and cant get new one
            throw new IllegalStateException( "Don't have a port to obtain a write result, and existing one is not good enough." );
        }

        return _lastErrorResult;
    }


    /**
     * Gets the error message from the {@code "err"} field).
     *
     * @return the error
     * @throws MongoException
     * @deprecated There should be no reason to use this method.  The error message will be in the exception thrown for an
     * unsuccessful write operation.
     */
    @Deprecated
    public String getError(){
        Object foo = getField( "err" );
        if ( foo == null )
            return null;
        return foo.toString();
    }
    
    /**
     * Gets the "n" field, which contains the number of documents
     * affected in the write operation.
     * @return the number of documents modified by the write operation
     * @throws MongoException
     */
    public int getN(){
        return getLastError().getInt( "n" );
    }

    /**
     * Gets the _id value of an upserted document that resulted from this write.  Note that for MongoDB servers prior to version 2.6,
     * this method will return null unless the _id of the upserted document was of type ObjectId.
     *
     * @return the value of the _id of an upserted document
     * @since 2.12
     */
    public Object getUpsertedId() {
        return getLastError().get("upserted");
    }


    /**
     * Returns true if this write resulted in an update of an existing document.
     *
     * @return whether the write resulted in an update of an existing document.
     * @since 2.12
     */
    public boolean isUpdateOfExisting() {
        return getLastError().getBoolean("updatedExisting");
    }


    /**
     * Gets a field from the response to the write operation.
     *
     * @param name field name
     * @return the value of the field with the given name
     * @throws MongoException
     * @deprecated There should be no reason to use this method.  To get specific fields from a successful write,
     * use the helper methods provided.  Any error-related fields will be in the exception thrown for an unsuccessful write operation.
     * @see #getN()
     * @see #getUpsertedId()
     * @see #isUpdateOfExisting()
     */
    @Deprecated
    public Object getField( String name ){
        return getLastError().get( name );
    }

    /**
     * Returns whether or not the result is lazy, meaning that getLastError was not called automatically
     * @return true if the result is lazy
     * @deprecated there is no replacement for this method
     */
    @Deprecated
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

// DBConnector.java

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


/**
 * Interface that provides the ability to exchange request/response with the database
 *
 * @deprecated This class is NOT part of the public API. It will be dropped in 3.x releases.
 */
@Deprecated
public interface DBConnector {

    /**
     * initiates a "consistent request" on the thread.
     * Once this has been called, the connector will ensure that the same underlying connection is always used for a given thread.
     * This happens until requestStop() is called.
     */
    public void requestStart();
    /**
     * terminates the "consistent request".
     */
    public void requestDone();
    /**
     * Ensures that a connection exists for the "consistent request"
     */
    public void requestEnsureConnection();
    
    /**
     * does a write operation
     * @param db the database
     * @param m the request message
     * @param concern the write concern
     * @return the write result
     * @throws MongoException
     */
    public WriteResult say( DB db , OutMessage m , WriteConcern concern );
    /**
     * does a write operation
     * @param db the database
     * @param m the request message
     * @param concern the write concern
     * @param hostNeeded specific server to connect to
     * @return the write result
     * @throws MongoException
     */
    public WriteResult say( DB db , OutMessage m , WriteConcern concern , ServerAddress hostNeeded );
    
    /**
     * does a read operation on the database
     * @param db the database
     * @param coll the collection
     * @param m the request message
     * @param hostNeeded specific server to connect to
     * @param decoder the decoder to use
     * @return the read result
     * @throws MongoException
     */
    public Response call( DB db , DBCollection coll , OutMessage m , 
                          ServerAddress hostNeeded , DBDecoder decoder );
    /**
     *
     * does a read operation on the database
     * @param db the database
     * @param coll the collection
     * @param m the request message
     * @param hostNeeded specific server to connect to
     * @param retries the number of retries in case of an error
     * @return the read result
     * @throws MongoException
     */
    public Response call( DB db , DBCollection coll , OutMessage m , ServerAddress hostNeeded , int retries );

    /**
     * does a read operation on the database
     * @param db the database
     * @param coll the collection
     * @param m the request message
     * @param hostNeeded specific server to connect to
     * @param retries number of retries in case of error
     * @param readPref the read preferences
     * @param decoder the decoder to use
     * @return the read result
     * @throws MongoException
     */
    public Response call( DB db , DBCollection coll , OutMessage m , ServerAddress hostNeeded , int retries , ReadPreference readPref , DBDecoder decoder );

    /**
     * returns true if the connector is in a usable state
     * @return
     */
    public boolean isOpen();

    /**
     * Authenticate using the given credentials.
     *
     * @param credentials the credentials.
     * @return the result of the authentication command, if successful
     * @throws CommandFailureException if the authentication failed
     * @since 2.11.0
     */
    public CommandResult authenticate(MongoCredential credentials);
}

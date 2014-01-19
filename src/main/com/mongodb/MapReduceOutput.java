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

package com.mongodb;

/**
 * Represents the result of a map/reduce operation.  Users should interact with the results of the map reduce via the results() method, or
 * by interacting directly with the collection the results were input into.
 * <p/>
 * There will be substantial changes to this class in the 3.x release, please check the deprecation tags for the methods that will be
 * removed.
 *
 * @mongodb.driver.manual reference/command/mapReduce/ Map Reduce Command
 */
public class MapReduceOutput {

    /**
     * Creates a new MapReduceOutput.
     *
     * @param from the DBCollection the Map Reduce was run against
     * @param cmd  the original Map Reduce command as a DBObject
     * @param raw  the CommandResult containing the raw response from the server.
     * @deprecated In the 3.0 version of the driver, this will be constructed only by the driver, and will therefore not have a public
     * constructor.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public MapReduceOutput( DBCollection from , DBObject cmd, CommandResult raw ){
        _commandResult = raw;
        _cmd = cmd;

        if ( raw.containsField( "results" ) ) {
            _coll = null;
            _collname = null;
            _resultSet = (Iterable<DBObject>) raw.get( "results" );
        } else {
            Object res = raw.get("result");
            if (res instanceof String) {
                _collname = (String) res;
            } else {
                BasicDBObject output = (BasicDBObject) res;
                _collname = output.getString("collection");
                _dbname = output.getString("db");
            }

            DB db = from._db;
            if (_dbname != null) {
                db = db.getSisterDB(_dbname);
            }
            _coll = db.getCollection( _collname );
            // M/R only applies to master, make sure we dont go to slave for results
            _coll.setOptions(_coll.getOptions() & ~Bytes.QUERYOPTION_SLAVEOK);
            _resultSet = _coll.find();
        }
        _counts = (BasicDBObject)raw.get( "counts" );
    }

    /**
     * Returns an iterable containing the results of the operation.
     *
     * @return the results in iterable form
     */
    public Iterable<DBObject> results(){
        return _resultSet;
    }

    /**
     * Drops the collection that holds the results.  Does nothing if the map-reduce returned the results inline.
     *
     * @throws MongoException
     */
    public void drop(){
        if ( _coll != null)
            _coll.drop();
    }

    /**
     * Gets the collection that holds the results (Will return null if results are Inline).
     *
     * @return the collection or null
     */
    public DBCollection getOutputCollection(){
        return _coll;
    }

    /**
     * Get the full document containing the raw results returned by the server.
     *
     * @return The result of the map-reduce operation as a document
     * @deprecated It is not recommended to access the raw document returned by the server as the format can change between releases. This
     * has been replaced with a series of specific getters for the values on the result (getCollectionName, getDatabaseName, getDuration,
     * getEmitCount, getOutputCount, getInputCount).  The method {@code results()} will continue to return an {@code Iterable<DBObjects>},
     * that should be used to obtain the results of the map-reduce.  This method will be removed in 3.0.
     */
    @Deprecated
    public BasicDBObject getRaw(){
        return _commandResult;
    }

    /**
     * The CommandResult should not be used directly at all, this will be removed.
     *
     * @return a CommandResult representing the output of the map-reduce in its raw form from the server.
     * @deprecated It is not recommended to access the command result returned by the server as the format can change between releases. This
     * has been replaced with a series of specific getters for the values on the CommandResult (getCollectionName, getDatabaseName,
     * getDuration, getEmitCount, getOutputCount, getInputCount).  The method {@code results()} will continue to return an {@code
     * Iterable<DBObjects>}, that should be used to obtain the results of the map-reduce.  This method will be removed in 3.0.
     */
    @Deprecated
    public CommandResult getCommandResult() {
        return _commandResult;
    }

    /**
     * Get the original command that was sent to the database.
     * @return a DBObject containing the values of the original map-reduce command.
     */
    public DBObject getCommand() {
        return _cmd;
    }

    /**
     * Get the server that the map reduce command was run on.
     *
     * @return a ServerAddress of the server that the command ran against.
     */
    public ServerAddress getServerUsed() {
        return _commandResult.getServerUsed();
    }

    public String toString(){
        return _commandResult.toString();
    }

    /**
     * Get the name of the collection that the results of the map reduce were saved into.  If the map reduce was an inline operation (i.e .
     * the results were returned directly from calling the map reduce) this will return null.
     *
     * @return the name of the collection that the map reduce results are stored in
     */
    public final String getCollectionName() {
        return _collname;
    }

    /**
     * Get the name of the database that the results of the map reduce were saved into.  If the map reduce was an inline operation (i.e .
     * the results were returned directly from calling the map reduce) this will return null.
     *
     * @return the name of the database that holds the collection that the map reduce results are stored in
     */
    public String getDatabaseName() {
        return _dbname;
    }

    /**
     * Get the amount of time, in milliseconds, that it took to run this map reduce.
     *
     * @return an int representing the number of milliseconds it took to run the map reduce operation
     */
    public int getDuration() {
        return _commandResult.getInt("timeMillis");
    }

    /**
     * Get the number of documents that were input into the map reduce operation
     *
     * @return the number of documents that read while processing this map reduce
     */
    public int getInputCount() {
        return _counts.getInt("input");
    }

    /**
     * Get the number of documents generated as a result of this map reduce
     *
     * @return the number of documents output by the map reduce
     */
    public int getOutputCount() {
        return _counts.getInt("output");
    }

    /**
     * Get the number of messages emitted from the provided map function.
     *
     * @return the number of items emitted from the map function
     */
    public int getEmitCount() {
        return _counts.getInt("emit");
    }

    final CommandResult _commandResult;
    final String _collname;
    String _dbname = null;
    final Iterable<DBObject> _resultSet;
    final DBCollection _coll;
    final BasicDBObject _counts;
    final DBObject _cmd;
}

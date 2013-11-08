/*
 * Copyright (c) 2008 - 2013 MongoDB, Inc. <http://mongodb.com>
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
 * Represents the result of a map/reduce operation
 * @author antoine
 */
public class MapReduceOutput {

    /**
     * In the 3.0 version of the driver, this will be constructed only by the driver, and will therefore not have a public constructor.
     * @param from the DBCollection the Map Reduce was run against
     * @param cmd the original Map Reduce command as a DBObject
     * @param raw the CommandResult containing the raw response from the server.
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
     * returns a cursor to the results of the operation
     * @return
     */
    public Iterable<DBObject> results(){
        return _resultSet;
    }

    /**
     * drops the collection that holds the results
     * @throws MongoException
     */
    public void drop(){
        if ( _coll != null)
            _coll.drop();
    }

    /**
     * gets the collection that holds the results
     * (Will return null if results are Inline)
     * @return
     */
    public DBCollection getOutputCollection(){
        return _coll;
    }

    @Deprecated
    public BasicDBObject getRaw(){
        return _commandResult;
    }

    /**
     * This has been replaced with a series of specific getters for the values on the CommandResult (getCollectionName, getDatabaseName,
     * getDuration, getEmitCount, getOutputCount, getInputCount).  The method {@code results()} will continue to return an
     * {@code Iterable<DBObjects>}, that should be used to obtain the results of the Map Reduce.  The CommandResult should not be
     * used directly at all.
     * @return
     */
    @Deprecated
    public CommandResult getCommandResult(){
        return _commandResult;
    }

    public DBObject getCommand() {
        return _cmd;
    }

    public ServerAddress getServerUsed() {
        return _commandResult.getServerUsed();
    }

    public String toString(){
        return _commandResult.toString();
    }

    public final String getCollectionName() {
        return _collname;
    }

    public String getDatabaseName() {
        return _dbname;
    }

    public int getDuration() {
        return (Integer) _commandResult.get("timeMillis");
    }

    public int getInputCount() {
        return _counts.getInt("input");
    }

    public int getOutputCount() {
        return _counts.getInt("output");
    }

    public int getEmitCount() {
        return _counts.getInt("emit");
    }

    @Deprecated
    final CommandResult _commandResult;

    final String _collname;
    String _dbname = null;
    final Iterable<DBObject> _resultSet;
    final DBCollection _coll;
    final BasicDBObject _counts;
    final DBObject _cmd;
}

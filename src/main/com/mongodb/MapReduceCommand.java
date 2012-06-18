/**
 * Copyright (c) 2010 10gen, Inc. <http://10gen.com>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.mongodb;

import java.util.Map;

/**
 * This class groups the argument for a map/reduce operation and can build the underlying command object
 * @dochub mapreduce
 */
public class MapReduceCommand {

    /**
     * INLINE - Return results inline, no result is written to the DB server
     * REPLACE - Save the job output to a collection, replacing its previous content
     * MERGE - Merge the job output with the existing contents of outputTarget collection
     * REDUCE - Reduce the job output with the existing contents of outputTarget collection
     */
    public static enum OutputType {
        REPLACE, MERGE, REDUCE, INLINE
    };

    /**
     * Represents the command for a map reduce operation
     * Runs the command in REPLACE output type to a named collection
     * 
     * @param inputCollection
     *            the collection to read from
     * @param map
     *            map function in javascript code
     * @param reduce
     *            reduce function in javascript code
     * @param outputCollection
     *            optional - leave null if want to get the result inline
     * @param type
     *            the type of output
     * @param query
     *            the query to use on input
     * @return
     * @dochub mapreduce
     */
    public MapReduceCommand(DBCollection inputCollection , String map , String reduce , String outputCollection, OutputType type, DBObject query) {
        _input = inputCollection.getName();
        _map = map;
        _reduce = reduce;
        _outputTarget = outputCollection;
        _outputType = type;
        _query = query;
    }

    /**
     * Sets the verbosity of the MapReduce job,
     * defaults to 'true'
     * 
     * @param verbose
     *            The verbosity level.
     */
    public void setVerbose( Boolean verbose ){
        _verbose = verbose;
    }

    /**
     * Gets the verbosity of the MapReduce job.
     * 
     * @return the verbosity level.
     */
    public Boolean isVerbose(){
        return _verbose;
    }

    /**
     * Get the name of the collection the MapReduce will read from
     * 
     * @return name of the collection the MapReduce will read from     
     */
    public String getInput(){
        return _input;
    }


    /**
     * Get the map function, as a JS String 
     * 
     * @return the map function (as a JS String)
     */
    public String getMap(){
        return _map;
    }

    /**
     * Gets the reduce function, as a JS String
     * 
     * @return the reduce function (as a JS String)
     */
    public String getReduce(){
        return _reduce;
    }

    /**
     * Gets the output target (name of collection to save to)
     * This value is nullable only if OutputType is set to INLINE
     * 
     * @return The outputTarget
     */
    public String getOutputTarget(){
        return _outputTarget;
    }


    /**
     * Gets the OutputType for this instance.
     * @return The outputType.
     */
    public OutputType getOutputType(){
        return _outputType;
    }


    /**
     * Gets the Finalize JS Function 
     * 
     * @return The finalize function (as a JS String).
     */
    public String getFinalize(){
        return _finalize;
    }

    /**
     * Sets the Finalize JS Function 
     * 
     * @param finalize
     *            The finalize function (as a JS String)
     */
    public void setFinalize( String finalize ){
        _finalize = finalize;
    }

    /**
     * Gets the query to run for this MapReduce job
     * 
     * @return The query object
     */
    public DBObject getQuery(){
        return _query;
    }

    /**
     * Gets the (optional) sort specification object 
     * 
     * @return the Sort DBObject
     */
    public DBObject getSort(){
        return _sort;
    }

    /**
     * Sets the (optional) sort specification object
     * 
     * @param sort
     *            The sort specification object
     */
    public void setSort( DBObject sort ){
        _sort = sort;
    }

    /**
     * Gets the (optional) limit on input
     * 
     * @return The limit specification object
     */
    public int getLimit(){
        return _limit;
    }

    /**
     * Sets the (optional) limit on input
     * 
     * @param limit
     *            The limit specification object
     */
    public void setLimit( int limit ){
        _limit = limit;
    }

    /**
     * Gets the (optional) JavaScript  scope 
     * 
     * @return The JavaScript scope
     */
    public Map<String, Object> getScope(){
        return _scope;
    }

    /**
     * Sets the (optional) JavaScript scope
     * 
     * @param scope
     *            The JavaScript scope
     */
    public void setScope( Map<String, Object> scope ){
        _scope = scope;
    }

    /**
     * Sets the (optional) database name where the output collection should reside
     * @param outputDB
     */
    public void setOutputDB(String outputDB) {
        this._outputDB = outputDB;
    }

    

    public DBObject toDBObject() {
        BasicDBObject cmd = new BasicDBObject();

        cmd.put("mapreduce", _input);
        cmd.put("map", _map);
        cmd.put("reduce", _reduce);
        cmd.put("verbose", _verbose);

        BasicDBObject out = new BasicDBObject();
        switch(_outputType) {
            case INLINE:
                out.put("inline", 1);
                break;
            case REPLACE:
                out.put("replace", _outputTarget);
                break;
            case MERGE:
                out.put("merge", _outputTarget);
                break;
            case REDUCE:
                out.put("reduce", _outputTarget);
                break;
        }
        if (_outputDB != null)
            out.put("db", _outputDB);
        cmd.put("out", out);

        if (_query != null)
            cmd.put("query", _query);

        if (_finalize != null) 
            cmd.put( "finalize", _finalize );

        if (_sort != null)
            cmd.put("sort", _sort);

        if (_limit > 0)
            cmd.put("limit", _limit);

        if (_scope != null)
            cmd.put("scope", _scope);

        if (_extra != null) {
            cmd.putAll(_extra);
        }
        
        return cmd;
    }

    public void addExtraOption(String name, Object value) {
        if (_extra == null)
            _extra = new BasicDBObject();
        _extra.put(name, value);
    }
    
    public DBObject getExtraOptions() {
        return _extra;
    }

    /**
     * Sets the read preference for this command.
     * See the * documentation for {@link ReadPreference}
     * for more information.
     *
     * @param preference Read Preference to use
     */
    public void setReadPreference( ReadPreference preference ){
        _readPref = preference;
    }
    
    /**
     * Gets the read preference
     * @return
     */
    public ReadPreference getReadPreference(){
        return _readPref;
    }
    
    
    public String toString() { 
        return toDBObject().toString();
    }

    final String _input;
    final String _map;
    final String _reduce;
    final String _outputTarget;
    ReadPreference _readPref;
    String _outputDB = null;
    final OutputType _outputType;
    final DBObject _query;
    String _finalize;
    DBObject _sort;
    int _limit;
    Map<String, Object> _scope;
    Boolean _verbose = true;
    DBObject _extra;
}

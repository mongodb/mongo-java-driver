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

public class MapReduceCommand {

    static enum OutputType {
        STANDARD, MERGE, REDUCE, INLINE
    };

    /**
     * Represents the command for a map reduce operation
     * Runs the command in STANDARD output to a named collection
     * 
     * @param input
     *            the collection to read from
     * @param map
     *            map function in javascript code
     * @param reduce
     *            reduce function in javascript code
     * @param outputTarget
     *            optional - leave null if want to use temp collection
     * @param query
     *            to match
     * @return
     * @throws MongoException
     * @dochub mapreduce
     */
    MapReduceCommand(String input , String map , String reduce , String outputTarget , DBObject query) throws MongoException {
        _input = input;
        _map = map;
        _reduce = reduce;
        _outputTarget = outputTarget;
        _outputType = OutputType.STANDARD;
        _query = query;
    }

    /**
     * Represents the command for a map reduce operation
     * Runs the command in INLINE output mode
     * 
     * @param input
     *            the collection to read from
     * @param map
     *            map function in javascript code
     * @param reduce
     *            reduce function in javascript code
     * @param query
     *            to match
     * @return
     * @throws MongoException
     * @dochub mapreduce
     */
    MapReduceCommand(String input , String map , String reduce , DBObject query) throws MongoException {
        _input = input;
        _map = map;
        _reduce = reduce;
        _outputTarget = null;
        _outputType = OutputType.INLINE;
        _query = query;
    }

    /**
     * Represents the command for a map reduce operation
     * 
     * @param input
     *            the collection to read from
     * @param map
     *            map function in javascript code
     * @param reduce
     *            reduce function in javascript code
     * @param outputTarget
     *            optional - leave null if want to use temp collection
     * @param outputType
     *            set the type of job output 
     * @param query
     *            to match
     * @return
     * @throws MongoException
     * @dochub mapreduce
     */
    MapReduceCommand(String input , String map , String reduce , String outputTarget , OutputType outputType , DBObject query) throws MongoException {
        _input = input;
        _map = map;
        _reduce = reduce;
        _outputTarget = outputTarget;
        _outputType = outputType;
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
     * INLINE - Return results inline
     * STANDARD - Save the job output to outputTarget
     * MERGE - Merge the job output with the existing contents of outputTarget
     * REDUCE - Reduce the job output with the existing contents of outputTarget
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
     * Gets the (optional) limit specification object
     * 
     * @return The limit specification object
     */
    public DBObject getLimit(){
        return _limit;
    }

    /**
     * Sets the (optional) limit specification object
     * 
     * @param limit
     *            The limit specification object
     */
    public void setLimit( DBObject limit ){
        _limit = limit;
    }

    /**
     * Gets the (optional) JavaScript  scope 
     * 
     * @return The JavaScript scope
     */
    public String getScope(){
        return _scope;
    }

    /**
     * Sets the (optional) JavaScript scope
     * 
     * @param scope
     *            The JavaScript scope
     */
    public void setScope( String scope ){
        _scope = scope;
    }

    DBObject toDBObject() {
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        builder.add("mapreduce", _input)
               .add("map", _map)
               .add("reduce", _reduce)
               .add("verbose", _verbose);

        switch(_outputType) {
            case INLINE: 
                builder.add("out", new BasicDBObject("inline", 1));
                break;
            case STANDARD: 
                builder.add("out", _outputTarget);
                break;
            case MERGE: 
                builder.add("out", new BasicDBObject("merge", _outputTarget));
                break;
            case REDUCE: 
                builder.add("out", new BasicDBObject("reduce", _outputTarget));
                break;
        }

        if (_query != null)
            builder.add("query", _query);

        if (_finalize != null) 
            builder.add( "finalize", _finalize );

        if (_sort != null)
            builder.add("sort", _sort);

        if (_limit != null)
            builder.add("limit", _limit);

        if (_scope != null)
            builder.add("scope", _scope);


        return builder.get();
    }

    public String toString() { 
        return toDBObject().toString();
    }

    final String _input;
    final String _map;
    final String _reduce;
    final String _outputTarget;
    final OutputType _outputType;
    final DBObject _query;
    String _finalize;
    DBObject _sort;
    DBObject _limit;
    String _scope;
    Boolean _verbose = true;
}

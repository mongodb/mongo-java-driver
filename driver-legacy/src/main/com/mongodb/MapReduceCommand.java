/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.client.model.Collation;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class groups the argument for a map/reduce operation and can build the underlying command object
 *
 * @mongodb.driver.manual applications/map-reduce Map-Reduce
 */
public class MapReduceCommand {

    private final String mapReduce;
    private final String map;
    private final String reduce;
    private String finalize;
    private ReadPreference readPreference;
    private final OutputType outputType;
    private final String outputCollection;
    private String outputDB;
    private final DBObject query;
    private DBObject sort;
    private int limit;
    private long maxTimeMS;
    private Map<String, Object> scope;
    private Boolean jsMode;
    private Boolean verbose;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    /**
     * Represents the command for a map reduce operation Runs the command in REPLACE output type to a named collection
     *
     * @param inputCollection  collection to use as the source documents to perform the map reduce operation.
     * @param map              a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce           a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputCollection optional - leave null if want to get the result inline
     * @param type             the type of output
     * @param query            specifies the selection criteria using query operators for determining the documents input to the map
     *                         function.
     * @mongodb.driver.manual reference/command/mapReduce/ Map Reduce Command
     */
    public MapReduceCommand(final DBCollection inputCollection, final String map, final String reduce, final String outputCollection,
                            final OutputType type, final DBObject query) {
        this.mapReduce = inputCollection.getName();
        this.map = map;
        this.reduce = reduce;
        this.outputCollection = outputCollection;
        this.outputType = type;
        this.query = query;
        this.outputDB = null;
        this.verbose = true;
    }

    /**
     * Sets the verbosity of the MapReduce job, defaults to 'true'
     *
     * @param verbose The verbosity level.
     */
    public void setVerbose(final Boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Gets the verbosity of the MapReduce job.
     *
     * @return the verbosity level.
     */
    public Boolean isVerbose() {
        return verbose;
    }

    /**
     * Get the name of the collection the MapReduce will read from
     *
     * @return name of the collection the MapReduce will read from
     */
    public String getInput() {
        return mapReduce;
    }


    /**
     * Get the map function, as a JS String
     *
     * @return the map function (as a JS String)
     */
    public String getMap() {
        return map;
    }

    /**
     * Gets the reduce function, as a JS String
     *
     * @return the reduce function (as a JS String)
     */
    public String getReduce() {
        return reduce;
    }

    /**
     * Gets the output target (name of collection to save to) This value is nullable only if OutputType is set to INLINE
     *
     * @return The outputCollection
     */
    public String getOutputTarget() {
        return outputCollection;
    }


    /**
     * Gets the OutputType for this instance.
     *
     * @return The outputType.
     */
    public OutputType getOutputType() {
        return outputType;
    }


    /**
     * Gets the Finalize JS Function
     *
     * @return The finalize function (as a JS String).
     */
    public String getFinalize() {
        return finalize;
    }

    /**
     * Sets the Finalize JS Function
     *
     * @param finalize The finalize function (as a JS String)
     */
    public void setFinalize(final String finalize) {
        this.finalize = finalize;
    }

    /**
     * Gets the query to run for this MapReduce job
     *
     * @return The query object
     */
    public DBObject getQuery() {
        return query;
    }

    /**
     * Gets the (optional) sort specification object
     *
     * @return the Sort DBObject
     */
    public DBObject getSort() {
        return sort;
    }

    /**
     * Sets the (optional) sort specification object
     *
     * @param sort The sort specification object
     */
    public void setSort(final DBObject sort) {
        this.sort = sort;
    }

    /**
     * Gets the (optional) limit on input
     *
     * @return The limit specification object
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the (optional) limit on input
     *
     * @param limit The limit specification object
     */
    public void setLimit(final int limit) {
        this.limit = limit;
    }

    /**
     * Gets the max execution time for this command, in the given time unit.
     *
     * @param timeUnit the time unit to return the value in.
     * @return the maximum execution time
     * @mongodb.server.release 2.6
     * @since 2.12.0
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    /**
     * Sets the max execution time for this command, in the given time unit.
     *
     * @param maxTime  the maximum execution time. A non-zero value requires a server version &gt;= 2.6
     * @param timeUnit the time unit that maxTime is specified in
     * @mongodb.server.release 2.6
     * @since 2.12.0
     */
    public void setMaxTime(final long maxTime, final TimeUnit timeUnit) {
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
    }

    /**
     * Gets the (optional) JavaScript  scope
     *
     * @return The JavaScript scope
     */
    public Map<String, Object> getScope() {
        return scope;
    }

    /**
     * Sets the (optional) JavaScript scope
     *
     * @param scope The JavaScript scope
     */
    public void setScope(final Map<String, Object> scope) {
        this.scope = scope;
    }

    /**
     * Gets the (optional) JavaScript mode
     *
     * @return The JavaScript mode
     * @since 2.13
     */
    public Boolean getJsMode() {
        return jsMode;
    }

    /**
     * Sets the (optional) JavaScript Mode
     *
     * @param jsMode Specifies whether to convert intermediate data into BSON format between the execution of the map and reduce functions
     * @since 2.13
     */
    public void setJsMode(final Boolean jsMode) {
        this.jsMode = jsMode;
    }

    /**
     * Gets the (optional) database name where the output collection should reside
     *
     * @return the name of the database the result is stored in, or null.
     */
    public String getOutputDB() {
        return this.outputDB;
    }

    /**
     * Sets the (optional) database name where the output collection should reside
     *
     * @param outputDB the name of the database to send the Map Reduce output to
     */
    public void setOutputDB(final String outputDB) {
        this.outputDB = outputDB;
    }

    /**
     * Gets whether to bypass document validation, or null if unspecified.  The default is null.
     *
     * @return whether to bypass document validation, or null if unspecified.
     * @since 2.14
     * @mongodb.server.release 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets whether to bypass document validation.
     *
     * @param bypassDocumentValidation whether to bypass document validation, or null if unspecified
     * @since 2.14
     * @mongodb.server.release 3.2
     */
    public void setBypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
    }

    /**
     * Turns this command into a DBObject representation of this map reduce command.
     *
     * @return a DBObject that contains the MongoDB document representation of this command.
     */
    public DBObject toDBObject() {
        BasicDBObject cmd = new BasicDBObject();

        cmd.put("mapreduce", mapReduce);
        cmd.put("map", map);
        cmd.put("reduce", reduce);

        if (verbose != null) {
            cmd.put("verbose", verbose);
        }

        BasicDBObject out = new BasicDBObject();
        switch (outputType) {
            case INLINE:
                out.put("inline", 1);
                break;
            case REPLACE:
                out.put("replace", outputCollection);
                break;
            case MERGE:
                out.put("merge", outputCollection);
                break;
            case REDUCE:
                out.put("reduce", outputCollection);
                break;
            default:
                throw new IllegalArgumentException("Unexpected output type");
        }
        if (outputDB != null) {
            out.put("db", outputDB);
        }
        cmd.put("out", out);

        if (query != null) {
            cmd.put("query", query);
        }

        if (finalize != null) {
            cmd.put("finalize", finalize);
        }

        if (sort != null) {
            cmd.put("sort", sort);
        }

        if (limit > 0) {
            cmd.put("limit", limit);
        }

        if (scope != null) {
            cmd.put("scope", scope);
        }

        if (jsMode != null) {
            cmd.put("jsMode", jsMode);
        }

        if (maxTimeMS != 0) {
            cmd.put("maxTimeMS", maxTimeMS);
        }

        return cmd;
    }

    /**
     * Sets the read preference for this command. See the * documentation for {@link ReadPreference} for more information.
     *
     * @param preference Read Preference to use
     */
    public void setReadPreference(final ReadPreference preference) {
        this.readPreference = preference;
    }

    /**
     * Gets the read preference
     *
     * @return the readPreference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Returns the collation
     *
     * @return the collation
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * @param collation the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public void setCollation(final Collation collation) {
        this.collation = collation;
    }

    @Override
    public String toString() {
        return toDBObject().toString();
    }

    /**
     * Represents the different options available for outputting the results of a map-reduce operation.
     *
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-out-cmd Output options
     */
    public enum OutputType {
        /**
         * Save the job output to a collection, replacing its previous content
         */
        REPLACE,
        /**
         * Merge the job output with the existing contents of outputTarget collection
         */
        MERGE,
        /**
         * Reduce the job output with the existing contents of outputTarget collection
         */
        REDUCE,
        /**
         * Return results inline, no result is written to the DB server
         */
        INLINE
    }

}

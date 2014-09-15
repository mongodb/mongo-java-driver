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

package com.mongodb.operation;

import org.bson.BsonDocument;
import org.bson.BsonJavaScript;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>A class that groups arguments for a map-reduce operation.</p>
 *
 * <p>This class follows a builder pattern.</p>
 *
 * @mongodb.driver.manual core/map-reduce Map-Reduce
 * @since 3.0
 */
public class MapReduce {
    private final BsonJavaScript mapFunction;
    private final BsonJavaScript reduceFunction;
    private final MapReduceOutputOptions output;
    private final boolean inline;
    private BsonJavaScript finalizeFunction;
    private BsonDocument scope;
    private BsonDocument filter;
    private BsonDocument sortCriteria;
    private int limit;
    private boolean jsMode;
    private boolean verbose;
    private long maxTimeMS;

    /**
     * Construct a new instance of the {@code MapReduce} with output to a another collection.
     *
     * @param mapFunction    a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     * @param output         specifies the location of the result of the map-reduce operation.
     */
    public MapReduce(final BsonJavaScript mapFunction, final BsonJavaScript reduceFunction, final MapReduceOutputOptions output) {
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.output = output;
        this.inline = output == null;
    }

    /**
     * Construct a new instance of the {@code MapReduce}. Operation will be performed in memory and the resulting documents will be
     * returned as a part of the response to the command call without storing them in third-party collection.
     *
     * @param mapFunction    a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     */
    public MapReduce(final BsonJavaScript mapFunction, final BsonJavaScript reduceFunction) {
        this(mapFunction, reduceFunction, null);
    }

    /**
     * <p>Add a finalize function to the command.</p>
     *
     * <p>The finalize function receives as its arguments a key value and the reducedValue from the reduce function. Be aware that:</p> 
     * <ul>
     *     <li>The finalize function should not access the database for any reason.</li> 
     *     <li>The finalize function should be pure, or have no impact outside of the function (i.e. side effects.)</li> 
     *     <li>The finalize function can access the variables defined in the scope parameter.</li> 
     * </ul>
     *
     * @param finalize a JavaScript function
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual manual/reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize function
     */
    public MapReduce finalize(final BsonJavaScript finalize) {
        this.finalizeFunction = finalize;
        return this;
    }

    /**
     * Add a filter to the command. It specifies the selection criteria using query operators for determining the documents input to the map
     * function.
     *
     * @param filter the selection criteria document.
     * @return {@code this} so calls can be chained
     */
    public MapReduce filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Sorts the input documents. This option is useful for optimization. For example, specify the sort key to be the same as the emit key
     * so that there are fewer reduce operations. The sort key must be in an existing index for this collection.
     *
     * @param sortCriteria sort criteria document
     * @return {@code this} so calls can be chained
     */
    public MapReduce sort(final BsonDocument sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    /**
     * Add global variables that will be accessible in the map, reduce and the finalize functions.
     *
     * @param scope scope document
     * @return {@code this} so calls can be chained
     */
    public MapReduce scope(final BsonDocument scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Specify a maximum number of documents to return from the input collection.
     *
     * @param limit limit value
     * @return {@code this} so calls can be chained
     */
    public MapReduce limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * <p>Add a 'jsMode' flag to the command.</p>
     *
     * <p>If set, internally the JavaScript objects emitted during map function remain as JavaScript objects. There will be no need to
     * convert the objects for the reduce function, which can result in faster execution.</p>
     *
     * @return {@code this} so calls can be chained
     */
    public MapReduce jsMode() {
        this.jsMode = true;
        return this;
    }

    /**
     * <p>Add a 'verbose' flag to the command.</p>
     *
     * <p>This flag specifies whether to include the timing information in the result information.</p>
     *
     * @return {@code this} so calls can be chained
     */
    public MapReduce verbose() {
        this.verbose = true;
        return this;
    }

    /**
     * This specifies a cumulative time limit for processing operations on the cursor. MongoDB interrupts the operation at the earliest
     * following interrupt point.
     *
     * @param maxTime  the time limit for processing the map reduce operation
     * @param timeUnit the TimeUnit for this limit
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual manual/reference/operator/meta/maxTimeMS/ $maxTimeMS
     * @mongodb.server.release 2.6
     */
    public MapReduce maxTime(final long maxTime, final TimeUnit timeUnit) {
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the map function for this map reduce operation.
     *
     * @return a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     */
    public BsonJavaScript getMapFunction() {
        return mapFunction;
    }

    /**
     * Gets the reduce function for this map reduce operation.
     *
     * @return a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     */
    public BsonJavaScript getReduceFunction() {
        return reduceFunction;
    }

    /**
     * Gets the finalize function for this map reduce operation
     *
     * @return a JavaScript function
     * @mongodb.driver.manual manual/reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize function
     */
    public BsonJavaScript getFinalizeFunction() {
        return finalizeFunction;
    }

    /**
     * Gets the filter criteria for this map reduce operation.
     *
     * @return the selection criteria document.
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Gets the criteria by which the data is sorted.
     *
     * @return sortCriteria sort criteria document
     */
    public BsonDocument getSortCriteria() {
        return sortCriteria;
    }

    /**
     * Gets the scope document for this map reduce operation.
     *
     * @return a document containing the variables that will be accessible in the map, reduce and the finalize functions.
     */
    public BsonDocument getScope() {
        return scope;
    }

    /**
     * Gets a MapReduceOutputOptions containing details of where the output of this map reduce operation will be.
     *
     * @return the location of the result of the map-reduce operation.
     */
    public MapReduceOutputOptions getOutput() {
        return output;
    }

    /**
     * Gets the limit value.
     *
     * @return the maximum number of documents to return from the input collection.
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Gets whether to convert intermediate data into BSON format or keep in JavaScript format between the execution of the map and reduce
     * functions.
     *
     * @return true if the JavaScript objects emitted during map function are to remain as JavaScript objects
     */
    public boolean isJsMode() {
        return jsMode;
    }

    /**
     * Gets whether the verbose flag is set or not.
     *
     * @return true if including the timing information in the result information.
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Gets whether the results are inline or not.
     *
     * @return true if the map-reduce operation is performed in memory and the result returned.
     */
    public boolean isInline() {
        return inline;
    }

    /**
     * Gets the max execution time for this map reduce command, in the given time unit.
     *
     * @param timeUnit the time unit to return the value in.
     * @return the maximum execution time
     * @mongodb.server.release 2.6
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    @Override
    public String toString() {
        return "MapReduce{"
               + "mapFunction=" + mapFunction
               + ", reduceFunction=" + reduceFunction
               + ", output=" + output
               + ", inline=" + inline
               + ", finalizeFunction=" + finalizeFunction
               + ", scope=" + scope
               + ", filter=" + filter
               + ", sortCriteria=" + sortCriteria
               + ", limit=" + limit
               + ", jsMode=" + jsMode
               + ", verbose=" + verbose
               + ", maxTimeMS" + maxTimeMS
               + '}';
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapReduce mapReduce = (MapReduce) o;

        if (inline != mapReduce.inline) {
            return false;
        }
        if (jsMode != mapReduce.jsMode) {
            return false;
        }
        if (limit != mapReduce.limit) {
            return false;
        }
        if (maxTimeMS != mapReduce.maxTimeMS) {
            return false;
        }
        if (verbose != mapReduce.verbose) {
            return false;
        }
        if (filter != null ? !filter.equals(mapReduce.filter) : mapReduce.filter != null) {
            return false;
        }
        if (finalizeFunction != null ? !finalizeFunction.equals(mapReduce.finalizeFunction) : mapReduce.finalizeFunction != null) {
            return false;
        }
        if (mapFunction != null ? !mapFunction.equals(mapReduce.mapFunction) : mapReduce.mapFunction != null) {
            return false;
        }
        if (output != null ? !output.equals(mapReduce.output) : mapReduce.output != null) {
            return false;
        }
        if (reduceFunction != null ? !reduceFunction.equals(mapReduce.reduceFunction) : mapReduce.reduceFunction != null) {
            return false;
        }
        if (scope != null ? !scope.equals(mapReduce.scope) : mapReduce.scope != null) {
            return false;
        }
        if (sortCriteria != null ? !sortCriteria.equals(mapReduce.sortCriteria) : mapReduce.sortCriteria != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = mapFunction != null ? mapFunction.hashCode() : 0;
        result = 31 * result + (reduceFunction != null ? reduceFunction.hashCode() : 0);
        result = 31 * result + (output != null ? output.hashCode() : 0);
        result = 31 * result + (inline ? 1 : 0);
        result = 31 * result + (finalizeFunction != null ? finalizeFunction.hashCode() : 0);
        result = 31 * result + (scope != null ? scope.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (sortCriteria != null ? sortCriteria.hashCode() : 0);
        result = 31 * result + limit;
        result = 31 * result + (jsMode ? 1 : 0);
        result = 31 * result + (verbose ? 1 : 0);
        result = 31 * result + (int) (maxTimeMS ^ (maxTimeMS >>> 32));
        return result;
    }

}

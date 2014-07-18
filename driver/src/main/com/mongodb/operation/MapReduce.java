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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A class that groups arguments for a map-reduce operation.
 * <p/>
 * This class follows a builder pattern.
 *
 * @mongodb.driver.manual core/map-reduce Map-Reduce
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
     * Constructs a new instance of the {@code MapReduce} with output to a another collection.
     *
     * @param mapFunction    a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     * @param output         specifies the location of the result of the map-reduce operation.
     */
    public MapReduce(final BsonJavaScript mapFunction, final BsonJavaScript reduceFunction, final MapReduceOutputOptions output) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.output = output;
        this.inline = output == null;
    }

    /**
     * Constructs a new instance of the {@code MapReduce}. Operation will be performed in memory and the resulting documents will be
     * returned as a part of the response to the command call without storing them in third-party collection.
     *
     * @param mapFunction    a JavaScript function that associates or “maps” a value with a key and emits the key and value pair.
     * @param reduceFunction a JavaScript function that “reduces” to a single object all the values associated with a particular key.
     */
    public MapReduce(final BsonJavaScript mapFunction, final BsonJavaScript reduceFunction) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.inline = true;

        this.output = null;
    }

    /**
     * Add a finalize function to the command.
     * <p/>
     * The finalize function receives as its arguments a key value and the reducedValue from the reduce function. Be aware that: <ul>
     * <li>The finalize function should not access the database for any reason.</li> <li>The finalize function should be pure, or have no
     * impact outside of the function (i.e. side effects.)</li> <li>The finalize function can access the variables defined in the scope
     * parameter.</li> </ul>
     *
     * @param finalize a JavaScript function
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
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
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
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
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce sort(final BsonDocument sortCriteria) {
        this.sortCriteria = sortCriteria;
        return this;
    }

    /**
     * Add global variables that will be accessible in the map, reduce and the finalize functions.
     *
     * @param scope scope document
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce scope(final BsonDocument scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Specify a maximum number of documents to return from the input collection.
     *
     * @param limit limit value
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Add a 'jsMode' flag to the command.
     * <p/>
     * This flag specifies whether to convert intermediate data into BSON format between the execution of the map and reduce functions
     *
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce jsMode() {
        this.jsMode = true;
        return this;
    }

    /**
     * Add a 'verbose' flag to the command.
     * <p/>
     * This flag specifies whether to include the timing information in the result information.
     *
     * @return the same {@code MapReduce} instance as used for the method invocation for chaining
     */
    public MapReduce verbose() {
        this.verbose = true;
        return this;
    }

    public MapReduce maxTime(final long maxTime, final TimeUnit timeUnit) {
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }


    public BsonJavaScript getMapFunction() {
        return mapFunction;
    }

    public BsonJavaScript getReduceFunction() {
        return reduceFunction;
    }

    public BsonJavaScript getFinalizeFunction() {
        return finalizeFunction;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public BsonDocument getSortCriteria() {
        return sortCriteria;
    }

    public BsonDocument getScope() {
        return scope;
    }

    public MapReduceOutputOptions getOutput() {
        return output;
    }

    public int getLimit() {
        return limit;
    }

    public boolean isJsMode() {
        return jsMode;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isInline() {
        return inline;
    }

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
}

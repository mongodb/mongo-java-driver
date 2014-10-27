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

package com.mongodb.client.model;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to apply to a map reduce operation
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/mapReduce/ mapReduce Command
 * @mongodb.driver.manual core/map-reduce/ mapReduce Overview
 */
public class MapReduceOptions {

    private final boolean inline;
    private final String collectionName;
    private String finalizeFunction;
    private Object scope;
    private Object filter;
    private Object sort;
    private int limit;
    private boolean jsMode;
    private boolean verbose = true;
    private long maxTimeMS;
    private Action action;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;

    /**
     * Constructs a new instance
     *
     * Sets the options for an inline map-reduce
     */
    public MapReduceOptions() {
        this.collectionName = null;
        this.inline = true;
    }

    /**
     * Constructs a new instance
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     */
    public MapReduceOptions(final String collectionName) {
        this.collectionName = notNull("collectionName", collectionName);
        this.inline = false;
    }

    /**
     * Is the output of the map reduce is inline
     *
     * @return if the output of the map reduce is inline
     */
    public boolean isInline() {
        return inline;
    }

    /**
     * Gets the name of the collection to output the results to or null if {@link #isInline}.
     *
     * @return the name of the collection to output the results to or null if {@link #isInline}.
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Gets the JavaScript function that follows the reduce method and modifies the output. Default is null
     *
     * @return the JavaScript function that follows the reduce method and modifies the output.
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize Function
     */
    public String getFinalizeFunction() {
        return finalizeFunction;
    }

    /**
     * Sets the JavaScript function that follows the reduce method and modifies the output.
     *
     * @param finalizeFunction the JavaScript function that follows the reduce method and modifies the output.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize Function
     */
    public MapReduceOptions finalizeFunction(final String finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    /**
     * Gets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @return the global variables that are accessible in the map, reduce and finalize functions.
     * @mongodb.driver.manual reference/command/mapReduce Scope
     */
    public Object getScope() {
        return scope;
    }

    /**
     * Sets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @param scope the global variables that are accessible in the map, reduce and finalize functions.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    public MapReduceOptions scope(final Object scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public Object getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public MapReduceOptions sort(final Object sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter to apply to the query.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public MapReduceOptions filter(final Object filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public Object getFilter() {
        return filter;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public MapReduceOptions limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
     * functions. Defaults to false.
     *
     * @return jsMode
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    public boolean isJsMode() {
        return jsMode;
    }

    /**
     * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
     * functions. Defaults to false.
     *
     * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and
     *               reduce functions
     * @return jsMode
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    public MapReduceOptions jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    /**
     * Gets whether to include the timing information in the result information. Defaults to true.
     *
     * @return whether to include the timing information in the result information
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Sets whether to include the timing information in the result information.
     *
     * @param verbose whether to include the timing information in the result information.
     * @return this
     */
    public MapReduceOptions verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public MapReduceOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the {@code Action} to be used when writing to a collection.
     *
     * @return the {@code Action} to be used when writing to a collection.
     */
    public Action getAction() {
        return action;
    }

    /**
     * Specify the {@code Action} to be used when writing to a collection.
     *
     * @param action an {@link Action} to perform on the collection
     * @return this
     */
    public MapReduceOptions action(final Action action) {
        this.action = notNull("action", action);
        return this;
    }

    /**
     * Gets the name of the database to output into.
     *
     * @return the name of the database to output into.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the name of the database to output into.
     *
     * @param databaseName the name of the database to output into.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceOptions databaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * True if the output database is sharded
     *
     * @return true if the output database is sharded
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public boolean isSharded() {
        return sharded;
    }

    /**
     * Sets if the output database is sharded
     *
     * @param sharded if the output database is sharded
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceOptions sharded(final boolean sharded) {
        this.sharded = sharded;
        return this;
    }

    /**
     * True if the post-processing step will prevent MongoDB from locking the database.
     *
     * @return if true the post-processing step will prevent MongoDB from locking the database.
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public boolean isNonAtomic() {
        return nonAtomic;
    }

    /**
     * Sets if the post-processing step will prevent MongoDB from locking the database.
     *
     * Valid only with the {@code Action.MERGE} or {@code Action.REDUCE} actions.
     *
     * @param nonAtomic if the post-processing step will prevent MongoDB from locking the database.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    public MapReduceOptions nonAtomic(final boolean nonAtomic) {
        this.nonAtomic = nonAtomic;
        return this;
    }

    /**
     * This option is only available when passing out a collection that already exists. This option is not available on secondary members of
     * replica sets.  The Enum values dictate what to do with the output collection if it already exists when the map reduce is run.
     */
    public static enum Action {
        /**
         * Replace the contents of the {@code collectionName} if the collection with the {@code collectionName} exists.
         */
        REPLACE("replace"),

        /**
         * Merge the new result with the existing result if the output collection already exists. If an existing document has the same key
         * as the new result, overwrite that existing document.
         */
        MERGE("merge"),

        /**
         * Merge the new result with the existing result if the output collection already exists. If an existing document has the same key
         * as the new result, apply the reduce function to both the new and the existing documents and overwrite the existing document with
         * the result.
         */
        REDUCE("reduce");

        private final String value;

        private Action(final String value) {
            this.value = value;
        }

        /**
         * @return the String representation of this Action that the MongoDB server understands
         */
        public String getValue() {
            return value;
        }
    }

}

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

/**
 * Specifies the location of the result of the map-reduce operation. You can output to a collection, output to a collection with an action,
 * or output inline. You may output to a collection when performing map reduce operations on the primary members of the set; on secondary
 * members you may only use the <b>inline</b> output.
 * <p/>
 * This class defines all the options if the output is not inline.  For results that are returned inline, a MapReduceOutput is not
 * required.
 * <p/>
 * This class follows a builder pattern.
 *
 * @mongodb.driver.manual reference/command/mapReduce/#out-options Out Options for Map-Reduce
 */
public class MapReduceOutputOptions {

    private final String collectionName;
    private Action action;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;

    /**
     * Constructs a new instance of the {@code MapReduceOutput}.
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     */
    public MapReduceOutputOptions(final String collectionName) {
        this.collectionName = collectionName;
        this.action = Action.REPLACE;
    }

    /**
     * Specify the name of the database that you want the map-reduce operation to write its output.
     *
     * @param databaseName the name of the database.
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutputOptions database(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Specify the {@code Action} to be used when writing to a collection that already exists.
     *
     * @param action an {@link Action} to perform on the Collection
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutputOptions action(final Action action) {
        this.action = action;
        return this;
    }

    /**
     * Add a 'sharded' flag.
     * <p/>
     * If specified and you have enabled sharding on output database, the map-reduce operation will shard the output collection using the
     * _id field as the shard key.
     *
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutputOptions sharded() {
        this.sharded = true;
        return this;
    }


    /**
     * Add a 'nonAtomic' flag. Valid only together with {@code Action.MERGE} and {@code Action.REDUCE}
     * <p/>
     * If specified the post-processing step will prevent MongoDB from locking the database; however, other clients will be able to read
     * intermediate states of the output collection. Otherwise the map reduce operation must lock the database during post-processing.
     *
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutputOptions nonAtomic() {
        this.nonAtomic = true;
        return this;
    }

    /**
     * @return the name of the collection to output into
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * @return the Action determining what to do if the output collection already exists
     */
    public Action getAction() {
        return action;
    }

    /**
     * @return the name of the Database that the output collection is in
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @return true if the the output database is sharded
     */
    public boolean isSharded() {
        return sharded;
    }

    /**
     * @return if true the post-processing step will prevent MongoDB from locking the database
     */
    public boolean isNonAtomic() {
        return nonAtomic;
    }

    /**
     * This option is only available when passing out a collection that already exists. This option is not available on secondary members of
     * replica sets.  The Enum values dictate what to do with the output collection if it already exists when the map reduce is run.
     */
    public static enum Action {
        /**
         * Replace the contents of the <collectionName> if the collection with the <collectionName> exists.
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

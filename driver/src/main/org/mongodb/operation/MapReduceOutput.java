/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation;

/**
 * Specifies the location of the result of the map-reduce operation.
 * You can output to a collection, output to a collection with an action, or output inline.
 * You may output to a collection when performing map reduce operations on the primary members of the set;
 * on secondary members you may only use the <b>inline</b> output.
 * <p/>
 * This class follows a builder pattern.
 *
 * @mongodb.driver.manual reference/command/mapReduce/#out-options Out Options for Map-Reduce
 */
public class MapReduceOutput {

    private String collectionName;
    private Action action;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;


    /**
     * Constructs a new instance of the {@code MapReduceOutput}.
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     */
    public MapReduceOutput(final String collectionName) {
        this.collectionName = collectionName;
        this.action = Action.REPLACE;
    }

    //CHECKSTYLE:OFF

    /**
     * Specify the name of the database that you want the map-reduce operation to write its output.
     *
     * @param databaseName the name of the database.
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput database(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Specify the {@code Action} to be used when writing to a collection that already exists.
     *
     * @param action
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput action(final Action action) {
        this.action = action;
        return this;
    }
    //CHECKSTYLE:ON

    /**
     * Add a 'sharded' flag.
     * <p/>
     * If specified and you have enabled sharding on output database, the map-reduce operation will
     * shard the output collection using the _id field as the shard key.
     *
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput sharded() {
        this.sharded = true;
        return this;
    }


    /**
     * Add a 'nonAtomic' flag. Valid only together with {@code Action.MERGE} and {@code Action.REDUCE}
     * <p/>
     * If specified the post-processing step will prevent MongoDB from locking the database;
     * however, other clients will be able to read intermediate states of the output collection.
     * Otherwise the map reduce operation must lock the database during post-processing.
     *
     * @return the same {@code MapReduceOutput} instance as used for the method invocation for chaining
     */
    public MapReduceOutput nonAtomic() {
        this.nonAtomic = true;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Action getAction() {
        return action;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public boolean isSharded() {
        return sharded;
    }

    public boolean isNonAtomic() {
        return nonAtomic;
    }

    public static enum Action {
        /**
         * Replace the contents of the <collectionName> if the collection with the <collectionName> exists.
         */
        REPLACE("replace"),

        /**
         * Merge the new result with the existing result if the output collection already exists.
         * If an existing document has the same key as the new result, overwrite that existing document.
         */
        MERGE("merge"),

        /**
         * Merge the new result with the existing result if the output collection already exists.
         * If an existing document has the same key as the new result, apply the reduce function
         * to both the new and the existing documents and overwrite the existing document with the result.
         */
        REDUCE("reduce");

        private String value;

        private Action(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}

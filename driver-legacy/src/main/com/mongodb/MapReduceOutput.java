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

import com.mongodb.internal.operation.MapReduceBatchCursor;
import com.mongodb.internal.operation.MapReduceStatistics;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the result of a map/reduce operation.  Users should interact with the results of the map reduce via the results() method, or
 * by interacting directly with the collection the results were input into.
 *
 * @mongodb.driver.manual applications/map-reduce Map-Reduce
 */
public class MapReduceOutput {

    private final DBCollection collection;
    private final DBObject command;
    private final List<DBObject> inlineResults;
    private final MapReduceStatistics mapReduceStatistics;
    private final DBCursor resultsFromCollection;

    /**
     * Constructor for use with inline map reduce.  Collection will always be null.
     */
    MapReduceOutput(final DBObject command, final MapReduceBatchCursor<DBObject> results) {

        this.command = command;
        this.mapReduceStatistics = results.getStatistics();

        this.collection = null;
        this.resultsFromCollection = null;
        this.inlineResults = new ArrayList<DBObject>();
        while (results.hasNext()) {
            this.inlineResults.addAll(results.next());
        }
        results.close();
    }

    /**
     * Constructor for use when the map reduce output was put into a collection
     */
    MapReduceOutput(final DBObject command, final DBCursor resultsFromCollection, final MapReduceStatistics mapReduceStatistics,
                    final DBCollection outputCollection) {
        this.command = command;
        this.inlineResults = null;
        this.mapReduceStatistics = mapReduceStatistics;

        this.collection = outputCollection;
        this.resultsFromCollection = resultsFromCollection;
    }

    /**
     * Returns an iterable containing the results of the operation.
     *
     * @return the results in iterable form
     */
    @SuppressWarnings("unchecked")
    public Iterable<DBObject> results() {
        if (inlineResults != null) {
            return inlineResults;
        } else {
            return resultsFromCollection;
        }
    }

    /**
     * Drops the collection that holds the results.  Does nothing if the map-reduce returned the results inline.
     */
    public void drop() {
        if (collection != null) {
            collection.drop();
        }
    }

    /**
     * Gets the collection that holds the results (Will return null if results are Inline).
     *
     * @return the collection or null
     */
    public DBCollection getOutputCollection() {
        return collection;
    }

    /**
     * Get the original command that was sent to the database.
     *
     * @return a DBObject containing the values of the original map-reduce command.
     */
    public DBObject getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "MapReduceOutput{"
               + "collection=" + collection
               + ", command=" + command
               + ", inlineResults=" + inlineResults
               + ", resultsFromCollection=" + resultsFromCollection
               + '}';
    }

    /**
     * Get the name of the collection that the results of the map reduce were saved into.  If the map reduce was an inline operation (i.e .
     * the results were returned directly from calling the map reduce) this will return null.
     *
     * @return the name of the collection that the map reduce results are stored in
     */
    @Nullable
    public final String getCollectionName() {
        return collection == null ? null : collection.getName();
    }

    /**
     * Get the name of the database that the results of the map reduce were saved into.  If the map reduce was an inline operation (i.e .
     * the results were returned directly from calling the map reduce) this will return null.
     *
     * @return the name of the database that holds the collection that the map reduce results are stored in
     */
    public String getDatabaseName() {
        return collection.getDB().getName();
    }

    /**
     * Get the amount of time, in milliseconds, that it took to run this map reduce.
     *
     * @return an int representing the number of milliseconds it took to run the map reduce operation
     */
    public int getDuration() {
        return mapReduceStatistics.getDuration();
    }

    /**
     * Get the number of documents that were input into the map reduce operation
     *
     * @return the number of documents that read while processing this map reduce
     */
    public int getInputCount() {
        return mapReduceStatistics.getInputCount();
    }

    /**
     * Get the number of documents generated as a result of this map reduce
     *
     * @return the number of documents output by the map reduce
     */
    public int getOutputCount() {
        return mapReduceStatistics.getOutputCount();
    }

    /**
     * Get the number of messages emitted from the provided map function.
     *
     * @return the number of items emitted from the map function
     */
    public int getEmitCount() {
        return mapReduceStatistics.getEmitCount();
    }
}
